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

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.ExpressionGenerators;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.foundationdb.qp.operator.API.*;

public class DistinctCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDBBestCase(ROWS);
        for (int fields = 1; fields <= 5; fields++) {
            run(fields, "best case");
        }
        populateDBWorstCase(ROWS);
        for (int fields = 1; fields <= 5; fields++) {
            run(fields, "worst case");
        }
    }

    private void run(int fields, String label)
    {
        sort(fields, WARMUP_RUNS, null);
        sort(fields, MEASUREMENT_RUNS, label);
    }

    private void createSchema()
    {
        t = createTable(
            schemaName(), newTableName(),
            "c1 int",
            "c2 int",
            "c3 int",
            "c4 int",
            "c5 int");
        group = group(t);
        schema = SchemaCache.globalSchema(ais());
        tRowType = schema.tableRowType(table(t));
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    private void populateDBBestCase(int rows)
    {
        // First column unique
        for (int i = 0; i < rows; i++) {
            writeRow(t, i, 0, 0, 0, 0);
        }
    }

    private void populateDBWorstCase(int rows)
    {
        // ALl columns have the same value in all rows. Have to check all columns to determine distinctness.
        for (int id = 0; id < rows; id++) {
            writeRow(t, id, 0, 0, 0, 0, 0);
        }
    }

    private void sort(int fields, int runs, String label)
    {
        List<ExpressionGenerator> projectFields = new ArrayList<>();
        for (int f = 0; f < fields; f++) {
            projectFields.add(ExpressionGenerators.field(tRowType, f));
        }
        Operator setup =
            project_DefaultTest(
                groupScan_Default(group),
                tRowType,
                projectFields);
        TimeOperator timeSetup = new TimeOperator(setup);
        RowType inputRowType = setup.rowType();
        Operator distinct = distinct_Partial(setup, inputRowType);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(distinct, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long distinctNsec = stop - start - timeSetup.elapsedNsec();
        if (label != null) {
            // Report the difference
            double averageUsecPerRow = distinctNsec / (1000.0 * runs * ROWS);
            System.out.println(String.format("%s, %s fields: %s usec/row",
                                             label, fields, averageUsecPerRow));
        }
    }

    private static final int ROWS = 1000;
    private static final int WARMUP_RUNS = 100;
    private static final int MEASUREMENT_RUNS = 100;

    private int t;
    private Group group;
    private Schema schema;
    private RowType tRowType;
    private StoreAdapter adapter;
}
