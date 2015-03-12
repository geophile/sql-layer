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
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.ExpressionGenerators;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static com.foundationdb.qp.operator.API.*;

public class SortCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB(100000);
        // Unidirectional
        // run(1, 0x1);
        run(2, 0x3);
        run(3, 0x7);
        // One change of direction
        run(2, 0x2);
        run(3, 0x4);
        // Two changes of direction
        run(3, 0x5);
    }

    private void run(int sortFields, int ordering)
    {
        // Warmup
        sort(sortFields, ordering, FILLER_100_COLUMN, WARMUP_RUNS, 1, false);
        // Measurements
        sort(sortFields, ordering, FILLER_100_COLUMN, MEASUREMENT_RUNS, 100000, true);
        sort(sortFields, ordering, FILLER_200_COLUMN, MEASUREMENT_RUNS, 100000, true);
        sort(sortFields, ordering, FILLER_300_COLUMN, MEASUREMENT_RUNS, 100000, true);
        sort(sortFields, ordering, FILLER_400_COLUMN, MEASUREMENT_RUNS, 100000, true);
        sort(sortFields, ordering, FILLER_100_COLUMN, MEASUREMENT_RUNS, 10000, true);
        sort(sortFields, ordering, FILLER_200_COLUMN, MEASUREMENT_RUNS, 10000, true);
        sort(sortFields, ordering, FILLER_300_COLUMN, MEASUREMENT_RUNS, 10000, true);
        sort(sortFields, ordering, FILLER_400_COLUMN, MEASUREMENT_RUNS, 10000, true);
        sort(sortFields, ordering, FILLER_100_COLUMN, MEASUREMENT_RUNS, 1000, true);
        sort(sortFields, ordering, FILLER_200_COLUMN, MEASUREMENT_RUNS, 1000, true);
        sort(sortFields, ordering, FILLER_300_COLUMN, MEASUREMENT_RUNS, 1000, true);
        sort(sortFields, ordering, FILLER_400_COLUMN, MEASUREMENT_RUNS, 1000, true);
        sort(sortFields, ordering, FILLER_100_COLUMN, MEASUREMENT_RUNS, 100, true);
        sort(sortFields, ordering, FILLER_200_COLUMN, MEASUREMENT_RUNS, 100, true);
        sort(sortFields, ordering, FILLER_300_COLUMN, MEASUREMENT_RUNS, 100, true);
        sort(sortFields, ordering, FILLER_400_COLUMN, MEASUREMENT_RUNS, 100, true);
        sort(sortFields, ordering, FILLER_100_COLUMN, MEASUREMENT_RUNS, 10, true);
        sort(sortFields, ordering, FILLER_200_COLUMN, MEASUREMENT_RUNS, 10, true);
        sort(sortFields, ordering, FILLER_300_COLUMN, MEASUREMENT_RUNS, 10, true);
        sort(sortFields, ordering, FILLER_400_COLUMN, MEASUREMENT_RUNS, 10, true);
        sort(sortFields, ordering, FILLER_100_COLUMN, MEASUREMENT_RUNS, 1, true);
        sort(sortFields, ordering, FILLER_200_COLUMN, MEASUREMENT_RUNS, 1, true);
        sort(sortFields, ordering, FILLER_300_COLUMN, MEASUREMENT_RUNS, 1, true);
        sort(sortFields, ordering, FILLER_400_COLUMN, MEASUREMENT_RUNS, 1, true);
    }

    private void createSchema()
    {
        t = createTable(
            "schema", "t",
            /* 0 */ "id int not null",
            /* 1 */ "a int",
            /* 2 */ "b int",
            /* 3 */ "c int",
            /* 4 */ "filler100 varchar(100)",
            /* 5 */ "filler200 varchar(200)",
            /* 6 */ "filler300 varchar(300)",
            /* 7 */ "filler400 varchar(400)",
            "primary key(id)");
        group = group(t);
        schema = SchemaCache.globalSchema(ais());
        tRowType = schema.tableRowType(table(t));
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    private void populateDB(int rows)
    {
        for (int id = 0; id < rows; id++) {
            writeRow(t,
                    id,
                    random.nextInt(),
                    random.nextInt(),
                    random.nextInt(),
                    FILLER_100,
                    FILLER_200,
                    FILLER_300,
                    FILLER_400);
        }
    }

    // ordering is a bitmask. bit[i] is true iff ith field is ascending.
    private void sort(int sortFields,
                      int orderingMask,
                      int fillerColumn,
                      int runs,
                      int rows,
                      boolean report)
    {
        Operator setup =
            project_DefaultTest(
                limit_Default(
                    groupScan_Default(group),
                    rows),
                tRowType,
                Arrays.asList(ExpressionGenerators.field(tRowType, 0),
                              ExpressionGenerators.field(tRowType, 1),
                              ExpressionGenerators.field(tRowType, 2),
                              ExpressionGenerators.field(tRowType, 3),
                              ExpressionGenerators.field(tRowType, fillerColumn)));
        RowType inputRowType = setup.rowType();
        int sortComplexity = 0;
        Ordering ordering = ordering();
        for (int f = 0; f < sortFields; f++) {
            boolean ascending = (orderingMask & (1 << f)) != 0;
            ordering.append(ExpressionGenerators.field(inputRowType, f), ascending);
            boolean previousAscending = (orderingMask & (1 << (f - 1))) != 0;
            if (f > 0 && ascending != previousAscending) {
                sortComplexity++;
            }
        }
        Operator sort =
            sort_General(
                setup,
                inputRowType,
                ordering,
                SortOption.PRESERVE_DUPLICATES);
        long start;
        long stop;
        // Measure time for setup
        start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(setup, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null);
        }
        stop = System.nanoTime();
        long setupNsec = stop - start;
        // Measure time for complete plan
        start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(sort, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null);
        }
        stop = System.nanoTime();
        long planNsec = stop - start;
        if (report) {
            // Report the difference
            long sortNsec = planNsec - setupNsec;
            double averageUsecPerRow = sortNsec / (1000.0 * runs * rows);
            int rowSize =
                32 /* 4 int columns */ +
                (fillerColumn == FILLER_100_COLUMN ? 100 :
                 fillerColumn == FILLER_200_COLUMN ? 200 :
                 fillerColumn == FILLER_300_COLUMN ? 300 : 400);
            System.out.println(String.format("rows: %s, row size: %s, sort fields: %s, sort complexity: %s, %s usec/row",
                                             rows,
                                             rowSize,
                                             sortFields,
                                             sortComplexity,
                                             averageUsecPerRow));
        }
    }

    private static final String FILLER_100;
    private static final String FILLER_200;
    private static final String FILLER_300;
    private static final String FILLER_400;

    static {
        StringBuilder buffer = new StringBuilder(100);
        for (int i = 0; i < 100; i++) {
            buffer.append('x');
        }
        FILLER_100 = buffer.toString();
        FILLER_200 = FILLER_100 + FILLER_100;
        FILLER_300 = FILLER_200 + FILLER_100;
        FILLER_400 = FILLER_300 + FILLER_100;
    }

    private static final int FILLER_100_COLUMN = 4;
    private static final int FILLER_200_COLUMN = FILLER_100_COLUMN + 1;
    private static final int FILLER_300_COLUMN = FILLER_200_COLUMN + 1;
    private static final int FILLER_400_COLUMN = FILLER_300_COLUMN + 1;
    private static final int WARMUP_RUNS = 10000;
    private static final int MEASUREMENT_RUNS = 10;

    private final Random random = new Random();
    private int t;
    private Group group;
    private Schema schema;
    private RowType tRowType;
    private StoreAdapter adapter;
}
