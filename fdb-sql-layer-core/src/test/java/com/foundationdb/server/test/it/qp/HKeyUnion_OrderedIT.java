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
import com.foundationdb.qp.rowtype.*;
import com.foundationdb.server.api.dml.SetColumnSelector;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.fail;

// Single-branch testing. See MultiIndexCrossBranchIT for cross-branch testing.

public class HKeyUnion_OrderedIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null primary key",
            "x int",
            "y int");
        createIndex("schema", "parent", "x", "x");
        createIndex("schema", "parent", "y", "y");
        child = createTable(
            "schema", "child",
            "cid int not null primary key",
            "pid int",
            "z int",
            "grouping foreign key (pid) references parent(pid)");
        createIndex("schema", "child", "z", "z");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        parentPidIndexRowType = indexType(parent, "pid");
        parentXIndexRowType = indexType(parent, "x");
        parentYIndexRowType = indexType(parent, "y");
        childZIndexRowType = indexType(child, "z");
        coi = group(parent);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[]{
            // 0x: Both index scans empty
            // 1x: Left empty
            row(parent, 1000L, -1L, 12L),
            row(parent, 1001L, -1L, 12L),
            row(parent, 1002L, -1L, 12L),
            // 2x: Right empty
            row(parent, 2000L, 22L, -1L),
            row(parent, 2001L, 22L, -1L),
            row(parent, 2002L, 22L, -1L),
            // 3x: Both non-empty, and no overlap
            row(parent, 3000L, 31L, -1L),
            row(parent, 3001L, 31L, -1L),
            row(parent, 3002L, 31L, -1L),
            row(parent, 3003L, 9999L, 32L),
            row(parent, 3004L, 9999L, 32L),
            row(parent, 3005L, 9999L, 32L),
            // 4x: left contains right
            row(parent, 4000L, 44L, -1L),
            row(parent, 4001L, 44L, 44L),
            row(parent, 4002L, 44L, 44L),
            row(parent, 4003L, 44L, 9999L),
            // 5x: right contains left
            row(parent, 5000L, -1L, 55L),
            row(parent, 5001L, 55L, 55L),
            row(parent, 5002L, 55L, 55L),
            row(parent, 5003L, 9999L, 55L),
            // 6x: overlap but neither side contains the other
            row(parent, 6000L, -1L, 66L),
            row(parent, 6001L, -1L, 66L),
            row(parent, 6002L, 66L, 66L),
            row(parent, 6003L, 66L, 66L),
            row(parent, 6004L, 66L, 9999L),
            row(parent, 6005L, 66L, 9999L),
            // 7x: parent with no children
            row(parent, 7000L, 70L, 70L),
            // 8x: parent with children
            row(parent, 8000L, 88L, 88L),
            row(child, 800000L, 8000L, 88L),
            row(parent, 8001L, 88L, 88L),
            row(child, 800100L, 8001L, 88L),
            row(child, 800101L, 8001L, 88L),
            row(parent, 8002L, 88L, 88L),
            row(child, 800200L, 8002L, 88L),
            row(child, 800201L, 8002L, 88L),
            row(child, 800202L, 8002L, 88L),
            // 9x child with no parent
            row(child, 900000L, 9000L, 99L),
            // 12x right join (child on right)
            row(child, 1200000L, null, 12L),
        };
        use(db);
    }

    // IllegalArumentException tests

    @Test
    public void testInputNull()
    {
        try {
            hKeyUnion_Ordered(null,
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              1,
                              parentRowType);
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              null,
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testRowTypesNull()
    {
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              null,
                              parentYIndexRowType,
                              1,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              null,
                              1,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              1,
                              null);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testOrderingColumns()
    {
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              -1,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              3,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              -1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              3,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              -1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              2,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    // Runtime tests

    @Test
    public void test0x()
    {
        Operator plan = unionPxPy(0);
        String[] expected = new String[]{
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test1x()
    {
        Operator plan = unionPxPy(12);
        String[] expected = new String[]{
            pKey(1000L),
            pKey(1001L),
            pKey(1002L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test2x()
    {
        Operator plan = unionPxPy(22);
        String[] expected = new String[]{
            pKey(2000L),
            pKey(2001L),
            pKey(2002L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test3x()
    {
        Operator plan = unionPxPy(31);
        String[] expected = new String[]{
            pKey(3000L),
            pKey(3001L),
            pKey(3002L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
        plan = unionPxPy(32);
        expected = new String[]{
            pKey(3003L),
            pKey(3004L),
            pKey(3005L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test4x()
    {
        Operator plan = unionPxPy(44);
        String[] expected = new String[]{
            pKey(4000L),
            pKey(4001L),
            pKey(4002L),
            pKey(4003L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test5x()
    {
        Operator plan = unionPxPy(55);
        String[] expected = new String[]{
            pKey(5000L),
            pKey(5001L),
            pKey(5002L),
            pKey(5003L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test6x()
    {
        Operator plan = unionPxPy(66);
        String[] expected = new String[]{
            pKey(6000L),
            pKey(6001L),
            pKey(6002L),
            pKey(6003L),
            pKey(6004L),
            pKey(6005L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test7x()
    {
        Operator plan = unionPxCz(70);
        String[] expected = new String[]{
            pKey(7000L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test8x()
    {
        Operator plan = unionPxCz(88);
        String[] expected = new String[]{
            pKey(8000L),
            pKey(8001L),
            pKey(8002L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test9x()
    {
        Operator plan = unionPxCz(99);
        String[] expected = new String[]{
            pKey(9000L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test12x()
    {
        Operator plan = unionPxCz(12);
        String[] expected = new String[]{
            pKey(null),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCursor()
    {
        Operator plan = unionPxCz(88);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public boolean hKeyComparison()
            {
                return true;
            }

            @Override
            public String[] firstExpectedHKeys()
            {
                return new String[] {
                    pKey(8000L),
                    pKey(8001L),
                    pKey(8002L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private Operator unionPxPy(int key)
    {
        Operator plan =
            hKeyUnion_Ordered(
                indexScan_Default(
                    parentXIndexRowType,
                    parentXEq(key),
                    ordering(field(parentXIndexRowType, 1), true)),
                indexScan_Default(
                    parentYIndexRowType,
                    parentYEq(key),
                    ordering(field(parentYIndexRowType, 1), true)),
                parentXIndexRowType,
                parentYIndexRowType,
                1,
                1,
                1,
                parentRowType);
        return plan;
    }

    private Operator unionPxCz(int key)
    {
        Operator plan =
            hKeyUnion_Ordered(
                indexScan_Default(
                    parentXIndexRowType,
                    parentXEq(key),
                    ordering(field(parentXIndexRowType, 1), true)),
                indexScan_Default(
                    childZIndexRowType,
                    childZEq(key),
                    ordering(field(childZIndexRowType, 1), true,
                             field(childZIndexRowType, 2), true)),
                    parentXIndexRowType,
                    childZIndexRowType,
                    1,
                    2,
                    1,
                    parentRowType);
        return plan;
    }
    
    private IndexKeyRange parentXEq(long x)
    {
        IndexBound xBound = new IndexBound(row(parentXIndexRowType, x), new SetColumnSelector(0));
        return IndexKeyRange.bounded(parentXIndexRowType, xBound, true, xBound, true);
    }

    private IndexKeyRange parentYEq(long y)
    {
        IndexBound yBound = new IndexBound(row(parentYIndexRowType, y), new SetColumnSelector(0));
        return IndexKeyRange.bounded(parentYIndexRowType, yBound, true, yBound, true);
    }

    private IndexKeyRange childZEq(long z)
    {
        IndexBound zBound = new IndexBound(row(childZIndexRowType, z), new SetColumnSelector(0));
        return IndexKeyRange.bounded(childZIndexRowType, zBound, true, zBound, true);
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

    private String pKey(Long pid)
    {
        return String.format("{%d,%s}", parentRowType.table().getOrdinal(), hKeyValue(pid));
    }

    private int parent;
    private int child;
    private TableRowType parentRowType;
    private TableRowType childRowType;
    private IndexRowType parentPidIndexRowType;
    private IndexRowType parentXIndexRowType;
    private IndexRowType parentYIndexRowType;
    private IndexRowType childZIndexRowType;
}
