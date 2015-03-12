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
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import org.junit.Ignore;
import org.junit.Test;

import java.util.EnumSet;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;

// Testing Intersect_Ordered key comparisons, which are done at the Persistit level.

@Ignore
public class Intersect_OrderedByteArrayComparisonIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "test int", // test case
            // For left index scan
            "l1 int",
            "l2 varchar(10)",
            "l3 int",
            // For right index scan
            "r1 int",
            "r2 varchar(10)",
            "r3 int",
            "primary key(id)");
        createIndex("schema", "t", "idx_left", "test", "l1", "l2", "l3");
        createIndex("schema", "t", "idx_right", "test", "r1", "r2", "r3");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        leftIndexRowType = indexType(t, "test", "l1", "l2", "l3");
        rightIndexRowType = indexType(t, "test", "r1", "r2", "r3");
        coi = group(t);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[]{
            // 1: Comparisons need only examine k1 on a mismatch.
            row(t, 1000, 1, 500, "x", 999, 501, "x", 999),
            row(t, 1001, 1, 502, "x", 999, 502, "x", 999),
            row(t, 1002, 1, 502, "x", 999, 502, "x", 999),
            row(t, 1003, 1, 504, "x", 999, 503, "x", 999),
            // 2: k1 always equal, k2s differ in length
            row(t, 1008, 2, 500, "x", 999, 500, "xx", 999),
            row(t, 1009, 2, 500, "xxx", 999, 500, "xxx", 999),
            row(t, 1010, 2, 500, "xxx", 999, 500, "xxx", 999),
            row(t, 1011, 2, 500, "xxxxx", 999, 500, "xxxx", 999),
            // 3: k1, k2 always match, difference is in k3
            row(t, 1016, 3, 500, "x", 900, 500, "x", 901),
            row(t, 1017, 3, 500, "x", 902, 500, "x", 902),
            row(t, 1018, 3, 500, "x", 902, 500, "x", 902),
            row(t, 1019, 3, 500, "x", 904, 500, "x", 903),
        };
        use(db);
    }

    @Test
    public void test1()
    {
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new Row[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        dump(plan);
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, true, false);
        expected = new Row[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, false, false);
        expected = new Row[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, false, false);
        expected = new Row[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new Row[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, true, true);
        expected = new Row[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, false, true);
        expected = new Row[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, false, true);
        expected = new Row[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test2()
    {
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new Row[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, true, false);
        expected = new Row[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, false, false);
        expected = new Row[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, false, false);
        expected = new Row[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new Row[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, true, true);
        expected = new Row[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, false, true);
        expected = new Row[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, false, true);
        expected = new Row[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test3()
    {
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, true, false);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, false, false);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, false, false);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, true, true);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, false, true);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, false, true);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCursor()
    {
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return expected;
            }
        };
        testCursorLifecycle(plan, testCase);
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new Row[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return expected;
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private Operator intersectPlan(int testId, IntersectOption side, boolean k2Ascending, boolean skipScan)
    {
        Ordering leftOrdering = 
            ordering(field(leftIndexRowType, 0), true,  // test
                     field(leftIndexRowType, 1), true,  // l1
                     field(leftIndexRowType, 2), k2Ascending,  // l2
                     field(leftIndexRowType, 3), true,  // l3
                     field(leftIndexRowType, 4), true); // id
        Ordering rightOrdering = 
            ordering(field(rightIndexRowType, 0), true,  // test
                     field(rightIndexRowType, 1), true,  // r1
                     field(rightIndexRowType, 2), k2Ascending,  // r2
                     field(rightIndexRowType, 3), true,  // r3
                     field(rightIndexRowType, 4), true); // id
        boolean ascending[] = new boolean[]{true, k2Ascending, true};
        Operator plan =
            intersect_Ordered(
                    indexScan_Default(
                            leftIndexRowType,
                            eq(leftIndexRowType, testId),
                            leftOrdering),
                    indexScan_Default(
                            rightIndexRowType,
                            eq(rightIndexRowType, testId),
                            rightOrdering),
                    leftIndexRowType,
                    rightIndexRowType,
                    4,
                    4,
                    ascending,
                    JoinType.INNER_JOIN,
                    EnumSet.of(side,
                            skipScan ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN),
                    null);
        return plan;
    }

    private IndexKeyRange eq(IndexRowType indexRowType, long testId)
    {
        IndexBound testBound = new IndexBound(row(indexRowType, testId), new SetColumnSelector(0));
        return IndexKeyRange.bounded(indexRowType, testBound, true, testBound, true);
    }

    private Ordering ordering(Object... objects)
    {
        Ordering ordering = API.ordering();
        int i = 0;
        while (i < objects.length) {
            ExpressionGenerator expression = (ExpressionGenerator) objects[i++];
            Boolean ascending = (Boolean) objects[i++];
            ordering.append(expression, ascending);
        }
        return ordering;
    }

    private int t;
    private RowType tRowType;
    private IndexRowType leftIndexRowType;
    private IndexRowType rightIndexRowType;
    private Operator plan;
    private Row[] expected;
}
