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

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.types.value.Value;

import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;

public class TreeScanCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        run(CostModelColumn.intColumn("x"));
        run(CostModelColumn.varcharColumnGoodPrefixCompression("x", 10));
        run(CostModelColumn.varcharColumnGoodPrefixCompression("x", 100));
        run(CostModelColumn.varcharColumnGoodPrefixCompression("x", 1000));
        run(CostModelColumn.varcharColumnBadPrefixCompression("x", 10));
        run(CostModelColumn.varcharColumnBadPrefixCompression("x", 100));
        run(CostModelColumn.varcharColumnBadPrefixCompression("x", 1000));
    }

    private void run(CostModelColumn indexedColumn)
    {
        this.indexedColumn = indexedColumn;
        createSchema();
        populateDB(ROWS);
        runSequential(WARMUP_RUNS, 1, null);
        String label = indexedColumn.description();
        runSequential(MEASURED_RUNS, 1000, label);
        runRandom(MEASURED_RUNS, 1000, label);
        runSequential(MEASURED_RUNS, 500, label);
        runRandom(MEASURED_RUNS, 500, label);
        runSequential(MEASURED_RUNS, 250, label);
        runRandom(MEASURED_RUNS, 250, label);
        runSequential(MEASURED_RUNS, 100, label);
        runRandom(MEASURED_RUNS, 100, label);
        runSequential(MEASURED_RUNS, 50, label);
        runRandom(MEASURED_RUNS, 50, label);
        runSequential(MEASURED_RUNS, 25, label);
        runRandom(MEASURED_RUNS, 25, label);
        runSequential(MEASURED_RUNS, 10, label);
        runRandom(MEASURED_RUNS, 10, label);
        runSequential(MEASURED_RUNS, 9, label);
        runRandom(MEASURED_RUNS, 9, label);
        runSequential(MEASURED_RUNS, 8, label);
        runRandom(MEASURED_RUNS, 8, label);
        runSequential(MEASURED_RUNS, 7, label);
        runRandom(MEASURED_RUNS, 7, label);
        runSequential(MEASURED_RUNS, 6, label);
        runRandom(MEASURED_RUNS, 6, label);
        runSequential(MEASURED_RUNS, 5, label);
        runRandom(MEASURED_RUNS, 5, label);
        runSequential(MEASURED_RUNS, 4, label);
        runRandom(MEASURED_RUNS, 4, label);
        runSequential(MEASURED_RUNS, 3, label);
        runRandom(MEASURED_RUNS, 3, label);
        runSequential(MEASURED_RUNS, 2, label);
        runRandom(MEASURED_RUNS, 2, label);
        runSequential(MEASURED_RUNS, 1, label);
        runRandom(MEASURED_RUNS, 1, label);
        runSequential(MEASURED_RUNS, 100, label);
        runRandom(MEASURED_RUNS, 100, label);
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String tableName = newTableName();
        t = createTable(schemaName, tableName,
                        "id int not null",
                        indexedColumn.declaration(),
                        "primary key(id)");
        createIndex(schemaName, tableName, "idx", indexedColumn.name());
        schema = SchemaCache.globalSchema(ais());
        idxRowType = indexType(t, indexedColumn.name());
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB(int rows)
    {
        start = rows / 2;
        keys = new Object[MAX_SCAN];
        int k = 0;
        for (int id = 0; id < rows; id++) {
            Object key = indexedColumn.valueFor(id);
            writeRow(t, id, key);
            if (id >= start && k < keys.length) {
                keys[k++] = key;
            }
        }
    }

    private void runSequential(int runs, int sequentialAccessesPerRandom, String label)
    {
        IndexBound lo = new IndexBound(row(idxRowType, indexedColumn.valueFor(start)),
                                       new SetColumnSelector(0));
        IndexBound hi = new IndexBound(row(idxRowType, indexedColumn.valueFor(Integer.MAX_VALUE)),
                                       new SetColumnSelector(0));
        Ordering ordering = new Ordering();
        ordering.append(field(idxRowType, 0), true);
        IndexKeyRange keyRange = IndexKeyRange.bounded(idxRowType, lo, true, hi, true);
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            for (int s = 0; s < sequentialAccessesPerRandom; s++) {
                Row row = cursor.next();
                assert row != null;
            }
            cursor.close();
        }
        long end = System.nanoTime();
        if (label != null) {
            double averageUsec = (end - start) / (1000.0 * runs * sequentialAccessesPerRandom);
            System.out.println(String.format("SEQUENTIAL %s - %s:  %s usec/row", label, sequentialAccessesPerRandom, averageUsec));
        }
    }

    // Do random accesses instead of sequential. Nearby random accesses are potentially faster.
    private void runRandom(int runs, int sequentialAccessesPerRandom, String label)
    {
        ValuesHolderRow boundRow = new ValuesHolderRow(idxRowType);
        Value valueHolder = boundRow.valueAt(0);
        queryBindings.setRow(0, boundRow);
        IndexBound bound = new IndexBound(boundRow, new SetColumnSelector(0));
        IndexKeyRange keyRange = IndexKeyRange.bounded(idxRowType, bound, true, bound, true);
        Ordering ordering = new Ordering();
        ordering.append(field(idxRowType, 0), true);
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        long startTime = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            for (int s = 0; s < sequentialAccessesPerRandom; s++) {
                Object key = keys[s];
                if (key instanceof Integer) {
                    valueHolder.putInt64((long) ((Integer) key).intValue());
                } else {
                    valueHolder.putString((String) key, null);
                }
                cursor.openTopLevel();
                Row row = cursor.next();
                assert row != null;
                cursor.closeTopLevel();
            }
        }
        long endTime = System.nanoTime();
        if (label != null) {
            double averageUsec = (endTime - startTime) / (1000.0 * runs * sequentialAccessesPerRandom);
            System.out.println(String.format("RANDOM %s - %s:  %s usec/row", label, sequentialAccessesPerRandom, averageUsec));
        }
    }

    private static final int ROWS = 2000000; // Should create 3-level trees
    private static final int WARMUP_RUNS = 20000;
    private static final int MEASURED_RUNS = 10000;
    private static final int MAX_SCAN = 1000;

    private int t;
    private IndexRowType idxRowType;
    private int start;
    private Object[] keys;
    private CostModelColumn indexedColumn;
}
