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
package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;
import org.junit.Test;

import java.util.EnumSet;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SkipScanPerformanceIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "y int",
            "z int",
            "primary key(id)");
        createIndex("schema", "t", "idx_x", "x");
        createIndex("schema", "t", "idx_y", "y");
        createIndex("schema", "t", "idx_z", "z");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        tIdIndexRowType = indexType(t, "id");
        tXIndexRowType = indexType(t, "x");
        tYIndexRowType = indexType(t, "y");
        tZIndexRowType = indexType(t, "z");
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    private static final IntersectOption LEFT = IntersectOption.OUTPUT_LEFT;
    private static final IntersectOption RIGHT = IntersectOption.OUTPUT_RIGHT;

    private int t;
    private int child;
    private RowType tRowType;
    private RowType childRowType;
    private IndexRowType tIdIndexRowType;
    private IndexRowType tXIndexRowType;
    private IndexRowType tYIndexRowType;
    private IndexRowType tZIndexRowType;

    // Tests are in pairs, with different values of N. This proves that SEQUENTIAL_SCAN costs increase linearly with N,
    // but SKIP_SCAN costs are constant (due to the way the test data is constructed).

    @Test
    public void testIntersect100()
    {
        final int N = 100;
        final int X = 888;
        final int Y = 999;
        // Create a set of N rows. Each set is structured as follows:
        //     id[0]      888    999      null
        //     id[1]      888    null     null
        //     ...
        //     id[N-1]    888    999      null
        assertTrue(N > 1);
        db = new Row[N];
        for (int id = 0; id < N; id++) {
            Integer y = id == 0 || id == N - 1 ? Y : null;
            db[id] = row(t, id, X, y, null);
        }
        use(db);
        // N rows from left index scan, 2 from right, 1 to realize scan is done.
        testIntersect(X, Y, false, N + 3);
        // 6 with skip scan: 1st, 2nd and last record on left, 1st and last record on right, 1 to realize scan is done.
        testIntersect(X, Y, true, 6);
    }

    @Test
    public void testIntersect1000()
    {
        final int N = 1000;
        final int X = 888;
        final int Y = 999;
        // Create a set of N rows. Each set is structured as follows:
        //     id[0]      888    999      null
        //     id[1]      888    null     null
        //     ...
        //     id[N-1]    888    999      null
        assertTrue(N > 1);
        db = new Row[N];
        for (int id = 0; id < N; id++) {
            Integer y = id == 0 || id == N - 1 ? Y : null;
            db[id] = row(t, id, X, y, null);
        }
        use(db);
        // N rows from left index scan, 2 from right, 1 to realize scan is done.
        testIntersect(X, Y, false, N + 3);
        // 6 with skip scan: 1st, 2nd and last record on left, 1st and last record on right, 1 to realize scan is done.
        testIntersect(X, Y, true, 6);
    }

    @Test
    public void testTwoIntersects60()
    {
        final int N = 60;
        final int X = 777;
        final int Y = 888;
        final int Z = 999;
        // Create a set of N rows, N divisible by 3.
        // - Scan of x retrieves rows [0, 2N/3)
        // - Scan of y retrieves rows [N/3, N)
        // - Scan of z retrieves rows N/3-1, N/3, N/3+1
        assertTrue((N % 3) == 0);
        assertTrue(N / 3 >= 2);
        db = new Row[N];
        for (int id = 0; id < N; id++) {
            Integer x = id < 2 * N / 3 ? X : null;
            Integer y = id >= N / 3 ? Y : null;
            Integer z = abs(id - N/3) <= 1 ? Z : null;
            Row row = row(t, id, x, y, z);
            db[id] = row;
        }
        use(db);
        // x scan: N/3 + 3
        // y scan: 3
        // z scan: 3
        // Realize two intersects are done: 2
        testTwoIntersects(X, Y, Z, false, N / 3 + 11);
        // x scan: 1st row, 3 rows starting at N/3
        // y scan: 3 rows starting at N/3
        // z scan: 3 rows
        // Realize two intersects are done: 2
        testTwoIntersects(X, Y, Z, true, 12);
    }

    @Test
    public void testTwoIntersects600()
    {
        final int N = 600;
        final int X = 777;
        final int Y = 888;
        final int Z = 999;
        // Create a set of N rows, N divisible by 3.
        // - Scan of x retrieves rows [0, 2N/3)
        // - Scan of y retrieves rows [N/3, N)
        // - Scan of z retrieves rows N/3-1, N/3, N/3+1
        assertTrue((N % 3) == 0);
        assertTrue(N / 3 >= 2);
        db = new Row[N];
        for (int id = 0; id < N; id++) {
            Integer x = id < 2 * N / 3 ? X : null;
            Integer y = id >= N / 3 ? Y : null;
            Integer z = abs(id - N/3) <= 1 ? Z : null;
            Row row = row(t, id, x, y, z);
            db[id] = row;
        }
        use(db);
        // x scan: N/3 + 3
        // y scan: 3
        // z scan: 3
        // Realize two intersects are done: 2
        testTwoIntersects(X, Y, Z, false, N / 3 + 11);
        // x scan: 1st row, 3 rows starting at N/3
        // y scan: 3 rows starting at N/3
        // z scan: 3 rows
        // Realize two intersects are done: 2
        testTwoIntersects(X, Y, Z, true, 12);
    }

    private void testIntersect(int x, int y, boolean skipScan, int expectedIndexRows)
    {
        Operator plan = intersectXY(x, y, skipScan);
        Tap.reset("operator.*");
        Tap.setEnabled("operator.*", true);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        while (cursor.next() != null);
        TapReport[] reports = Tap.getReport("operator.*");
        for (TapReport report : reports) {
            if (report.getName().equals("operator: IndexScan_Default next")) {
                assertEquals(expectedIndexRows, report.getInCount());
            }
        }
    }

    private void testTwoIntersects(int x, int y, int z, boolean skipScan, int expectedIndexRows)
    {
        Operator plan = intersectXYintersectZ(x, y, z, skipScan);
        Tap.reset("operator.*");
        Tap.setEnabled("operator.*", true);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        Row row;
        while ((row = cursor.next()) != null);
        TapReport[] reports = Tap.getReport("operator.*");
        for (TapReport report : reports) {
            if (report.getName().equals("operator: IndexScan_Default next")) {
                assertEquals(expectedIndexRows, report.getInCount());
            }
        }
    }

    private Operator intersectXY(int x, int y, boolean skipScan)
    {
        Ordering xOrdering = new Ordering();
        xOrdering.append(field(tXIndexRowType, 1), true);
        Ordering yOrdering = new Ordering();
        yOrdering.append(field(tYIndexRowType, 1), true);
        return
            intersect_Ordered(
                    indexScan_Default(
                            tXIndexRowType,
                            xEq(x),
                            xOrdering),
                    indexScan_Default(
                            tYIndexRowType,
                            yEq(y),
                            yOrdering),
                    tXIndexRowType,
                    tYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(
                            skipScan ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN,
                            IntersectOption.OUTPUT_LEFT),
                    null,
                    true);
    }

    private Operator intersectXYintersectZ(int x, int y, int z, boolean skip)
    {
        Ordering xOrdering = new Ordering();
        xOrdering.append(field(tXIndexRowType, 1), true);
        Ordering yOrdering = new Ordering();
        yOrdering.append(field(tYIndexRowType, 1), true);
        Ordering zOrdering = new Ordering();
        zOrdering.append(field(tZIndexRowType, 1), true);
        IntersectOption scanType = skip ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN;
        return intersect_Ordered(
                intersect_Ordered(
                        indexScan_Default(
                                tXIndexRowType,
                                xEq(x),
                                xOrdering),
                        indexScan_Default(
                                tYIndexRowType,
                                yEq(y),
                                yOrdering),
                        tXIndexRowType,
                        tYIndexRowType,
                        1,
                        1,
                        ascending(true),
                        JoinType.INNER_JOIN,
                        EnumSet.of(scanType, LEFT),
                        null,
                        true),
                indexScan_Default(
                        tZIndexRowType,
                        zEq(z),
                        zOrdering),
                tXIndexRowType,
                tZIndexRowType,
                1,
                1,
                ascending(true),
                JoinType.INNER_JOIN,
                EnumSet.of(scanType, LEFT),
                null,
                true);
    }

    private Operator unionXXunionX(int x1, int x2, int x3)
    {
        Ordering ordering = new Ordering();
        ordering.append(field(tXIndexRowType, 1), true);
        return union_Ordered(
            union_Ordered(
                indexScan_Default(
                    tXIndexRowType,
                    xEq(x1),
                    ordering),
                indexScan_Default(
                    tXIndexRowType,
                    yEq(x2),
                    ordering),
                tXIndexRowType,
                tXIndexRowType,
                1,
                1,
                ascending(true),
                false),
            indexScan_Default(
                tXIndexRowType,
                xEq(x3),
                ordering),
            tXIndexRowType,
            tXIndexRowType,
            1,
            1,
            ascending(true),
            false);
    }

    private Operator intersectXYunionX(int x1, int y, int x2, boolean skip)
    {
        Ordering xOrdering = new Ordering();
        xOrdering.append(field(tXIndexRowType, 1), true);
        Ordering yOrdering = new Ordering();
        yOrdering.append(field(tYIndexRowType, 1), true);
        IntersectOption scanType = skip ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN;
        return union_Ordered(
            intersect_Ordered(
                    indexScan_Default(
                            tXIndexRowType,
                            xEq(x1),
                            xOrdering),
                    indexScan_Default(
                            tYIndexRowType,
                            yEq(y),
                            yOrdering),
                    tXIndexRowType,
                    tYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(scanType, LEFT),
                    null,
                    true),
            indexScan_Default(
                tXIndexRowType,
                xEq(x2),
                xOrdering),
            tXIndexRowType,
            tXIndexRowType,
            1,
            1,
            ascending(true),
            false);
    }

    private Operator unionXXintersectY(int x1, int x2, int y, IntersectOption intersectOutput, boolean skip)
    {
        Ordering xOrdering = new Ordering();
        xOrdering.append(field(tXIndexRowType, 1), true);
        Ordering yOrdering = new Ordering();
        yOrdering.append(field(tYIndexRowType, 1), true);
        IntersectOption scanType = skip ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN;
        return intersect_Ordered(
                union_Ordered(
                        indexScan_Default(
                                tXIndexRowType,
                                xEq(x1),
                                xOrdering),
                        indexScan_Default(
                                tXIndexRowType,
                                xEq(x2),
                                xOrdering),
                        tXIndexRowType,
                        tXIndexRowType,
                        1,
                        1,
                        ascending(true),
                        false),
                indexScan_Default(
                        tYIndexRowType,
                        yEq(y),
                        yOrdering),
                tXIndexRowType,
                tYIndexRowType,
                1,
                1,
                ascending(true),
                JoinType.INNER_JOIN,
                EnumSet.of(scanType, intersectOutput),
                null,
                true);
    }

    private IndexKeyRange xEq(long x)
    {
        IndexBound bound = new IndexBound(row(tXIndexRowType, x), new SetColumnSelector(0));
        return IndexKeyRange.bounded(tXIndexRowType, bound, true, bound, true);
    }

    private IndexKeyRange yEq(long y)
    {
        IndexBound bound = new IndexBound(row(tYIndexRowType, y), new SetColumnSelector(0));
        return IndexKeyRange.bounded(tYIndexRowType, bound, true, bound, true);
    }

    private IndexKeyRange zEq(long z)
    {
        IndexBound bound = new IndexBound(row(tZIndexRowType, z), new SetColumnSelector(0));
        return IndexKeyRange.bounded(tZIndexRowType, bound, true, bound, true);
    }

    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
}
