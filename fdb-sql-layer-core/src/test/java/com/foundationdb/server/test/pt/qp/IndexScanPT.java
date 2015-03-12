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
package com.foundationdb.server.test.pt.qp;

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.util.tap.Tap;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;

public class IndexScanPT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "primary key(id)");
        createIndex("schema", "t", "idx_x", "x");
        schema = SchemaCache.globalSchema(ais());
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "x");
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    @Test
    public void profileIndexScan()
    {
        Tap.setEnabled(".*", false);
        populateDB(ROWS);
        run(null, WARMUP_RUNS, 1);
        run("0", MEASURED_RUNS, 1);
/*
        run("1", MEASURED_RUNS, 2);
        run("2", MEASURED_RUNS, 3);
        run("3", MEASURED_RUNS, 4);
        run("4", MEASURED_RUNS, 5);
        run("5", MEASURED_RUNS, 6);
        run("6", MEASURED_RUNS, 7);
        run("7", MEASURED_RUNS, 8);
        run("8", MEASURED_RUNS, 9);
        run("9", MEASURED_RUNS, 10);
        run("10", MEASURED_RUNS, 11);
        run("25", MEASURED_RUNS, 26);
        run("50", MEASURED_RUNS, 51);
        run("100", MEASURED_RUNS, 101);
        run("250", MEASURED_RUNS, 251);
        run("500", MEASURED_RUNS, 501);
        run("1000", MEASURED_RUNS, 1001);
*/
    }

    private void run(String label, int runs, int sequentialAccessesPerRandom)
    {
        IndexBound lo = new IndexBound(row(idxRowType, Integer.MAX_VALUE / 2), new SetColumnSelector(0));
        IndexBound hi = new IndexBound(row(idxRowType, Integer.MAX_VALUE), new SetColumnSelector(0));
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
            cursor.closeTopLevel();
        }
        long end = System.nanoTime();
        if (label != null) {
            double averageMsec = (double) (end - start) / (1000 * runs);
            System.out.println(String.format("%s:  %s usec", label, averageMsec));
        }
    }

    protected void populateDB(int rows)
    {
        for (int id = 0; id < rows; id++) {
            int x = random.nextInt();
            writeRow(t, id, x);
        }
    }

    private static final int ROWS = 50000;
    private static final int WARMUP_RUNS = 20000;
    private static final int MEASURED_RUNS = 1000000000;

    private final Random random = new Random();
    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
}
