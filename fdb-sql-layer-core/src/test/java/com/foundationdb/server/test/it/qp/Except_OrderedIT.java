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

import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.SetWrongNumColumns;

import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.fail;

public class Except_OrderedIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
                "schema", "t",
                "pid int not null primary key",
                "x int");
        u = createTable(
                "schema", "u",
                "pid int not null primary key",
                "x int");
        v = createTable(
                "schema", "v",
                "pid int not null primary key",
                "x int");
        w = createTable(
                "schema", "w",
                "pid int not null primary key",
                "x int");
        createIndex("schema", "t", "idx_x", "x");
        createIndex("schema", "u", "idx_x", "x");
        createIndex("schema", "v", "idx_x", "x");
        createIndex("schema", "w", "idx_x", "x");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        uRowType = schema.tableRowType(table(u));
        tPidIndexRowType = indexType(t, "pid");
        tXIndexRowType = indexType(t, "x");
        uXIndexRowType = indexType(u, "x");
        vXIndexRowType = indexType(v, "x");
        wXIndexRowType = indexType(w, "x");
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        
        db = new Row[] {
                row(t, 1000L, 10L),
                row(t, 1001L, 20L),
                row(t, 1002L, 50L),
                row(t, 1003L, 80L),
                row(t, 1004L, 90L),
                row(t, 1005L, 90L),

                row(u, 1000L, 1L),
                row(u, 1001L, 2L),
                row(u, 1002L, 5L),
                row(u, 1003L, 8L),
                row(u, 1004L, 9L),
                row(u, 1005L, 9L),

                row(v, 1000L, 0L),
                row(v, 1001L, 1L),
                row(v, 1002L, 1L),
                row(v, 1003L, 2L),
                row(v, 1004L, 9L),
                row(v, 1005L, 20L),
        };
        use(db);
    }

    private int t,u,v, w;
    private RowType uRowType;
    private IndexRowType tPidIndexRowType;
    private IndexRowType tXIndexRowType,uXIndexRowType, vXIndexRowType, wXIndexRowType;

    // IllegalArgumentException tests

    @Test
    public void testInputNull()
    {
        // First input null
        try {
            except_Ordered(null,
                    groupScan_Default(coi),
                    tXIndexRowType,
                    tXIndexRowType,
                    1,
                    1,
                    ascending(true),
                    false);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Second input null
        try {
            except_Ordered(groupScan_Default(coi),
                    null,
                    tXIndexRowType,
                    tXIndexRowType,
                    1,
                    1,
                    ascending(true),
                    false);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testInputType()
    {
        // First input type null
        try {
            except_Ordered(groupScan_Default(coi),
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
            except_Ordered(groupScan_Default(coi),
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
    public void testDifferentInputTypes() {
        // Test different input types

            except_Ordered(groupScan_Default(coi),
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
            except_Ordered(groupScan_Default(coi),
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
            except_Ordered(groupScan_Default(coi),
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
            except_Ordered(groupScan_Default(coi),
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
            except_Ordered(groupScan_Default(coi),
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
            except_Ordered(groupScan_Default(coi),
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
        Operator plan = exceptPlan(wXIndexRowType, wXIndexRowType, true, false);
        Row[] expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan(wXIndexRowType, wXIndexRowType, false, false);
        expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan(wXIndexRowType, wXIndexRowType, true, true);
        expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan(wXIndexRowType, wXIndexRowType, false, true);
        expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testLeftEmpty()
    {
        Operator plan = exceptPlan( wXIndexRowType,tXIndexRowType, true, false);
        Row[] expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan( wXIndexRowType, vXIndexRowType, false, false);
        expected = new Row[] {

        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan( wXIndexRowType,tXIndexRowType, true, true);
        expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan( wXIndexRowType, vXIndexRowType, false, true);
        expected = new Row[] {

        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testRightEmpty()
    {
        Operator plan = exceptPlan( uXIndexRowType,wXIndexRowType, true, false);
        Row[] expected = new Row[] {
                row(uRowType, 1L, 1000L),
                row(uRowType, 2L, 1001L),
                row(uRowType, 5L, 1002L),
                row(uRowType, 8L, 1003L),
                row(uRowType, 9L, 1004L),
                row(uRowType, 9L, 1005L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan( uXIndexRowType,wXIndexRowType, false, false);
        expected = new Row[] {
                row(uRowType, 9L, 1005L),
                row(uRowType, 9L, 1004L),
                row(uRowType, 8L, 1003L),
                row(uRowType, 5L, 1002L),
                row(uRowType, 2L, 1001L),
                row(uRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan( uXIndexRowType,wXIndexRowType, true, true);
        expected = new Row[] {
                row(uRowType, 1L, 1000L),
                row(uRowType, 2L, 1001L),
                row(uRowType, 5L, 1002L),
                row(uRowType, 8L, 1003L),
                row(uRowType, 9L, 1004L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan( uXIndexRowType,wXIndexRowType, false, true);
        expected = new Row[] {
                row(uRowType, 9L, 1005L),
                row(uRowType, 8L, 1003L),
                row(uRowType, 5L, 1002L),
                row(uRowType, 2L, 1001L),
                row(uRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDuplicates()
    {
        Operator  plan = exceptPlan( tXIndexRowType,tXIndexRowType, true, false);
        Row[] expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));

        plan = exceptPlan( vXIndexRowType,vXIndexRowType, false, false);
        expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan( tXIndexRowType,tXIndexRowType, true, true);
        expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));

        plan = exceptPlan( vXIndexRowType,vXIndexRowType, false, true);
        expected = new Row[] {
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDisjoint_asc_duplicates()
    {
        Operator plan = exceptPlan( uXIndexRowType,tXIndexRowType, true, false);
        Row[] expected = new Row[] {
                row(uRowType, 1L, 1000L),
                row(uRowType, 2L, 1001L),
                row(uRowType, 5L, 1002L),
                row(uRowType, 8L, 1003L),
                row(uRowType, 9L, 1004L),
                row(uRowType, 9L, 1005L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    @Test
    public void testDisjoint_desc_duplicates()
    {

        Operator plan = exceptPlan( uXIndexRowType,tXIndexRowType, false, false);
        Row[] expected = new Row[] {
                row(uRowType, 9L, 1005L),
                row(uRowType, 9L, 1004L),
                row(uRowType, 8L, 1003L),
                row(uRowType, 5L, 1002L),
                row(uRowType, 2L, 1001L),
                row(uRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDisjoint_asc_noDulicates()
    {
        Operator plan = exceptPlan( uXIndexRowType,tXIndexRowType, true, true);
        Row[] expected = new Row[] {
                row(uRowType, 1L, 1000L),
                row(uRowType, 2L, 1001L),
                row(uRowType, 5L, 1002L),
                row(uRowType, 8L, 1003L),
                row(uRowType, 9L, 1004L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    
    @Test
    public void testDisjoint_desc_noDulicates()
    {
        Operator plan = exceptPlan( uXIndexRowType,tXIndexRowType, false, true);
        Row[] expected = new Row[] {
                row(uRowType, 9L, 1005L),
                row(uRowType, 8L, 1003L),
                row(uRowType, 5L, 1002L),
                row(uRowType, 2L, 1001L),
                row(uRowType, 1L, 1000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void multiCases()
    {
        Operator plan = exceptPlan(uXIndexRowType, vXIndexRowType, true, false);
        Row[] expected = new Row[] {

                row(uXIndexRowType, 5L, 1002L),
                row(uXIndexRowType, 8L, 1003L),
                row(uXIndexRowType, 9L, 1005L)
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));

        plan = exceptPlan(uXIndexRowType, vXIndexRowType, false, false);
        expected = new Row[] {
                row(uXIndexRowType, 9L, 1004L),
                row(uXIndexRowType, 8L, 1003L),
                row(uXIndexRowType, 5L, 1002L)
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = exceptPlan(uXIndexRowType, vXIndexRowType, true, false);
        expected = new Row[] {

                row(uXIndexRowType, 5L, 1002L),
                row(uXIndexRowType, 8L, 1003L),
                row(uXIndexRowType, 9L, 1005L)
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));

        plan = exceptPlan(uXIndexRowType, vXIndexRowType, false, false);
        expected = new Row[] {
                row(uXIndexRowType, 9L, 1004L),
                row(uXIndexRowType, 8L, 1003L),
                row(uXIndexRowType, 5L, 1002L)
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private Operator exceptPlan(IndexRowType t1, IndexRowType t2, boolean ascending, boolean removeDuplicates)
    {
        Operator plan =
                except_Ordered(
                        indexScan_Default(
                                t1,
                                IndexKeyRange.unbounded(t1),
                                ordering(field(t1, 0), ascending)),
                        indexScan_Default(
                                t2,
                                IndexKeyRange.unbounded(t2),
                                ordering(field(t2, 0), ascending)),
                        t1,
                        t2,
                        2,
                        2,
                        ascending(ascending),
                        removeDuplicates);
        return plan;
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
