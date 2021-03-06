/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2009-2015 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.foundationdb.server.test.costmodel;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.expression.RowBasedUnboundExpressions;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.ExpressionGenerators;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.foundationdb.qp.operator.API.*;

public class Select_BloomFilterCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        // Load-only
        run(WARMUP_RUNS, true, 0, false);
        run(MEASURED_RUNS, true, 0, true);
        // Load and scan. Vary the starting value to control the amount of overlap with the filter rows.
        run(WARMUP_RUNS, false, 0, false);
        for (int start = 0; start <= FILTER_ROWS; start += FILTER_ROWS / 10) {
            run(MEASURED_RUNS, false, start, true);
        }
    }

    private void createSchema() throws InvalidOperationException
    {
        // Schema is similar to that in Select_BloomFilterIT
        String schemaName = schemaName();
        String dTableName = newTableName(); // Driving table
        String fTableName = newTableName(); // Filtering table
        d = createTable(
            schemaName, dTableName,
            "x int");
        f = createTable(
            schemaName, fTableName,
            "x int");
        Index dx = createIndex(schemaName, dTableName, "idx_dx", "x");
        Index fx = createIndex(schemaName, fTableName, "idx_fx", "x");
        schema = SchemaCache.globalSchema(ais());
        dRowType = schema.tableRowType(table(d));
        fRowType = schema.tableRowType(table(f));
        dIndexRowType = dRowType.indexRowType(dx);
        fIndexRowType = fRowType.indexRowType(fx);
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB()
    {
        for (int x = 0; x < FILTER_ROWS; x++) {
            writeRow(f, x, x); // x, hidden_pk
        }
        for (int x = 0; x < DRIVING_ROWS; x++) {
            writeRow(d, x, x); // x, hidden_pk
        }
    }
    
    private void run(int runs, boolean loadOnly, int startScan, boolean report)
    {
        Operator plan = loadOnly ? planLoadOnly() : planLoadAndSelect(startScan);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                // System.out.println(row);
            }
        }
        long stop = System.nanoTime();
        long planNsec = stop - start;
        if (loadOnly) {
            planNsec -= timeFilterInput.elapsedNsec();
            if (report) {
                double averageUsecPerRow = planNsec / (1000.0 * runs * FILTER_ROWS);
                System.out.println(String.format("load only: %s usec/row", averageUsecPerRow));
            }
        } else {
            planNsec -= (timeFilterInput.elapsedNsec() + timeScanInput.elapsedNsec());
            if (report) {
                double averageUsecPerRow = planNsec / (1000.0 * runs * (DRIVING_ROWS - startScan));
                double selected = (double) (FILTER_ROWS - startScan) / DRIVING_ROWS;
                System.out.println(String.format("scan %s: %s usec/row", selected, averageUsecPerRow));
            }
        }
    }

    public Operator planLoadOnly()
    {
        // filterInput loads the filter with F rows containing the given testId.
        Operator filterInput =
            project_DefaultTest(
                groupScan_Default(group(f)),
                fRowType,
                Arrays.asList(ExpressionGenerators.field(fRowType, 0)));
        timeFilterInput = new TimeOperator(filterInput);
        // For the  index scan retrieving rows from the F(x) index given a D index row
        IndexBound fxBound = new IndexBound(
            new RowBasedUnboundExpressions(
                filterInput.rowType(),
                Arrays.asList(ExpressionGenerators.boundField(dIndexRowType, 0, 0)), true),
            new SetColumnSelector(0));
        IndexKeyRange fKeyRange = IndexKeyRange.bounded(fIndexRowType, fxBound, true, fxBound, true);
        // Use a bloom filter loaded by filterInput. Then for each input row, check the filter (projecting
        // D rows on (x)), and, for positives, check F using an index scan keyed by D.x.
        Operator plan =
            using_BloomFilter(
                // filterInput
                timeFilterInput,
                // filterRowType
                filterInput.rowType(),
                // estimatedRowCount
                FILTER_ROWS,
                // filterBindingPosition
                0,
                // streamInput
                select_BloomFilterTest(
                    // input
                    valuesScan_Default(Collections.<BindableRow>emptyList(), dIndexRowType),
                    // onPositive
                    indexScan_Default(
                        fIndexRowType,
                        fKeyRange,
                        new Ordering()),
                    // filterFields
                    Arrays.asList(ExpressionGenerators.field(dIndexRowType, 0)),
                    // filterBindingPosition, pipeline, depth
                    0, false, 1));
        return plan;
    }

    public Operator planLoadAndSelect(int start)
    {
        // filterInput loads the filter with F rows containing the given testId.
        Operator filterInput =
            project_DefaultTest(
                groupScan_Default(group(f)),
                fRowType,
                Arrays.asList(ExpressionGenerators.field(fRowType, 0)));
        timeFilterInput = new TimeOperator(filterInput);
        // For the index scan retriving rows from the D(x) index
        IndexBound dxLo =
            new IndexBound(row(dIndexRowType, start), new SetColumnSelector(0));
        IndexBound dxHi =
            new IndexBound(row(dIndexRowType, Integer.MAX_VALUE), new SetColumnSelector(0));
        IndexKeyRange dKeyRange =
            IndexKeyRange.bounded(dIndexRowType, dxLo, true, dxHi, false);
        // For the  index scan retrieving rows from the F(x) index given a D index row
        IndexBound fxBound = new IndexBound(
            new RowBasedUnboundExpressions(
                filterInput.rowType(),
                Arrays.asList(ExpressionGenerators.boundField(dIndexRowType, 0, 0)), true),
            new SetColumnSelector(0));
        IndexKeyRange fKeyRange = IndexKeyRange.bounded(fIndexRowType, fxBound, true, fxBound, true);
        // Use a bloom filter loaded by filterInput. Then for each input row, check the filter (projecting
        // D rows on (x)), and, for positives, check F using an index scan keyed by D.x.
        Operator scanInput = indexScan_Default(dIndexRowType, dKeyRange, new Ordering());
        timeScanInput = new TimeOperator(scanInput);
        Operator plan =
            using_BloomFilter(
                // filterInput
                timeFilterInput,
                // filterRowType
                filterInput.rowType(),
                // estimatedRowCount
                FILTER_ROWS,
                // filterBindingPosition
                0,
                // streamInput
                select_BloomFilterTest(
                    // input
                    timeScanInput,
                    // onPositive
                    indexScan_Default(
                        fIndexRowType,
                        fKeyRange,
                        new Ordering()),
                    // filterFields
                    Arrays.asList(ExpressionGenerators.field(dIndexRowType, 0)),
                    // filterBindingPosition, pipeline, depth
                    0, false, 1));
        return plan;
    }

    private static final int WARMUP_RUNS = 2;
    private static final int MEASURED_RUNS = 5;
    private static final long FILTER_ROWS = 100000;
    private static final long DRIVING_ROWS = 200000;

    private int d;
    private int f;
    private TableRowType dRowType;
    private TableRowType fRowType;
    private IndexRowType dIndexRowType;
    private IndexRowType fIndexRowType;
    private TimeOperator timeFilterInput;
    private TimeOperator timeScanInput;
}
