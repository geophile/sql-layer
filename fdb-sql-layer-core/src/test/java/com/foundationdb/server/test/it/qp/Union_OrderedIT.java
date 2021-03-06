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
import com.foundationdb.server.error.SetWrongNumColumns;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.fail;

public class Union_OrderedIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "pid int not null primary key",
            "x int");
        createIndex("schema", "t", "idx_x", "x");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        tPidIndexRowType = indexType(t, "pid");
        tXIndexRowType = indexType(t, "x");
        coi = group(t);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[] {
            row(t, 1000L, 1L),
            row(t, 1001L, 1L),
            row(t, 1002L, 1L),
            row(t, 2000L, 2L),
            row(t, 2001L, 2L),
            row(t, 2002L, 2L),
        };
        use(db);
    }

    private int t;
    private RowType tRowType;
    private IndexRowType tPidIndexRowType;
    private IndexRowType tXIndexRowType;

    // IllegalArumentException tests

    @Test
    public void testInputNull()
    {
        // First input null
        try {
            union_Ordered(null,
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          1,
                          ascending(true),
                          false);
        } catch (IllegalArgumentException e) {
        }
        // Second input null
        try {
            union_Ordered(groupScan_Default(coi),
                          null,
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          1,
                          ascending(true),
                          false);
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testInputType()
    {
        // First input type null
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          null,
                          tXIndexRowType,
                          1,
                          1,
                          ascending(true),
                          false);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Second input type null
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          null,
                          1,
                          1,
                          ascending(true),
                          false);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test (expected = SetWrongNumColumns.class)
    public void testDifferentInputTypes() 
    {
        // Test different input types
        union_Ordered(groupScan_Default(coi),
                      groupScan_Default(coi),
                      tXIndexRowType,
                      tPidIndexRowType,
                      1,
                      1,
                      ascending(true),
                      false);
    }
    
    @Test
    public void testOrderingColumns()
    {
        // First ordering fields negative
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          -1,
                          1,
                          ascending(true),
                          false);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Second ordering fields negative
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          -1,
                          ascending(true),
                          false);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // First ordering fields too high
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          3,
                          1,
                          ascending(true),
                          false);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Second ordering fields too high
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          3,
                          ascending(true),
                          false);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Different number of ordering fields
        try {
            union_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          tXIndexRowType,
                          tXIndexRowType,
                          1,
                          2,
                          ascending(true),
                          false);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    // Runtime tests

    @Test
    public void testBothInputsEmpty()
    {
        Operator plan = unionPlan(0, 0, true, false);
        Row[] expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = unionPlan(0, 0, false, false);
        expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testLeftEmpty()
    {
        Operator plan = unionPlan(0, 1, true, false);
        Row[] expected = new Row[] {
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = unionPlan(0, 1, false, false);
        expected = new Row[] {
            row(tRowType, 1L, 1002L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testRightEmpty()
    {
        Operator plan = unionPlan(1, 0, true, false);
        Row[] expected = new Row[] {
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = unionPlan(1, 0, false, false);
        expected = new Row[] {
            row(tRowType, 1L, 1002L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDuplicates()
    {
        Operator plan = unionPlan(1, 1, true, false);
        Row[] expected = new Row[] {
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = unionPlan(1, 1, false, false);
        expected = new Row[] {
            row(tRowType, 1L, 1002L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = unionPlan(1, 1, true, true);
        expected = new Row[] {
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1002L),
            row(tRowType, 1L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDisjoint()
    {
        Operator plan = unionPlan(1, 2, true, false);
        Row[] expected = new Row[] {
            row(tRowType, 1L, 1000L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1002L),
            row(tRowType, 2L, 2000L),
            row(tRowType, 2L, 2001L),
            row(tRowType, 2L, 2002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = unionPlan(1, 2, false, false);
        expected = new Row[] {
            row(tRowType, 2L, 2002L),
            row(tRowType, 2L, 2001L),
            row(tRowType, 2L, 2000L),
            row(tRowType, 1L, 1002L),
            row(tRowType, 1L, 1001L),
            row(tRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private Operator unionPlan(int k1, int k2, boolean ascending, boolean outputEqual)
    {
        Operator plan =
            union_Ordered(
                indexScan_Default(
                    tXIndexRowType,
                    parentXEq(k1),
                    ordering(field(tXIndexRowType, 1), ascending)),
                indexScan_Default(
                    tXIndexRowType,
                    parentXEq(k2),
                    ordering(field(tXIndexRowType, 1), ascending)),
                tXIndexRowType,
                tXIndexRowType,
                1,
                1,
                ascending(ascending),
                outputEqual);
        return plan;
    }

    private IndexKeyRange parentXEq(long x)
    {
        IndexBound xBound = new IndexBound(row(tXIndexRowType, x), new SetColumnSelector(0));
        return IndexKeyRange.bounded(tXIndexRowType, xBound, true, xBound, true);
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

    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
}
