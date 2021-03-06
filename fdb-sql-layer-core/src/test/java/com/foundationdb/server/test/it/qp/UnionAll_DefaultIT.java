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

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.texpressions.Comparison;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;

public class UnionAll_DefaultIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "primary key(id)");
        createIndex("schema", "t", "tx", "x");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        txIndexRowType = indexType(t, "x");
        tRowType = schema.tableRowType(table(t));
        groupTable = group(t);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[]{
            row(t, 1000L, 8L),
            row(t, 1001L, 9L),
            row(t, 1002L, 8L),
            row(t, 1003L, 9L),
            row(t, 1004L, 8L),
            row(t, 1005L, 9L),
            row(t, 1006L, 8L),
            row(t, 1007L, 9L),
        };
        use(db);
    }

    protected boolean openBoth() {
        return false;
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput1()
    {
        unionAll_Default(null, tRowType, groupScan_Default(groupTable), tRowType, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput1Type()
    {
        unionAll_Default(groupScan_Default(groupTable), null, groupScan_Default(groupTable), tRowType, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput2()
    {
        unionAll_Default(groupScan_Default(groupTable), tRowType, null, tRowType, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput2Type()
    {
        unionAll_Default(groupScan_Default(groupTable), tRowType, groupScan_Default(groupTable), null, false);
    }

    // Test operator execution

    @Test
    public void testBothEmpty()
    {
        Operator plan =
            unionAll_Default(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.literal(false)),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.literal(false)),
                tRowType,
                openBoth());
        Row[] expected = new Row[]{};
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testLeftEmpty()
    {
        Operator plan =
            unionAll_Default(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.literal(false)),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                tRowType,
                openBoth());
        Row[] expected = new Row[]{
            row(tRowType, 1001L, 9L),
            row(tRowType, 1003L, 9L),
            row(tRowType, 1005L, 9L),
            row(tRowType, 1007L, 9L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testRightEmpty()
    {
        Operator plan =
            unionAll_Default(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(8), castResolver())),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.literal(false)),
                tRowType, 
                openBoth());
        Row[] expected = new Row[]{
            row(tRowType, 1000L, 8L),
            row(tRowType, 1002L, 8L),
            row(tRowType, 1004L, 8L),
            row(tRowType, 1006L, 8L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testBothNonEmpty()
    {
        Operator plan =
            unionAll_Default(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(8), castResolver())),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                tRowType, 
                openBoth());
        Row[] expected = new Row[]{
            row(tRowType, 1000L, 8L),
            row(tRowType, 1002L, 8L),
            row(tRowType, 1004L, 8L),
            row(tRowType, 1006L, 8L),
            row(tRowType, 1001L, 9L),
            row(tRowType, 1003L, 9L),
            row(tRowType, 1005L, 9L),
            row(tRowType, 1007L, 9L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            unionAll_Default(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(8), castResolver())),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                tRowType, 
                openBoth());
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(tRowType, 1000L, 8L),
                    row(tRowType, 1002L, 8L),
                    row(tRowType, 1004L, 8L),
                    row(tRowType, 1006L, 8L),
                    row(tRowType, 1001L, 9L),
                    row(tRowType, 1003L, 9L),
                    row(tRowType, 1005L, 9L),
                    row(tRowType, 1007L, 9L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    // Inspired by bug972748. Also tests handling of index rows
    @Test
    public void testUADReuse()
    {
        IndexBound eight = new IndexBound(row(txIndexRowType, 8), new SetColumnSelector(0));
        IndexBound nine = new IndexBound(row(txIndexRowType, 9), new SetColumnSelector(0));
        IndexKeyRange xEQ8Range = IndexKeyRange.bounded(txIndexRowType, eight, true, eight, true);
        IndexKeyRange xEQ9Range = IndexKeyRange.bounded(txIndexRowType, nine, true, nine, true);
        Ordering ordering = new Ordering();
        ordering.append(field(txIndexRowType, 0), true);
        Operator plan =
            map_NestedLoops(
                limit_Default(
                    groupScan_Default(groupTable),
                    2),
                unionAll_Default(
                    indexScan_Default(txIndexRowType,
                                      xEQ8Range,
                                      ordering),
                    txIndexRowType,
                    indexScan_Default(txIndexRowType,
                                      xEQ9Range,
                                      ordering),
                    txIndexRowType, openBoth()),
                0, pipelineMap(), 1);
        String[] expected = new String[]{
            hKey(1000),
            hKey(1002),
            hKey(1004),
            hKey(1006),
            hKey(1001),
            hKey(1003),
            hKey(1005),
            hKey(1007),
            hKey(1000),
            hKey(1002),
            hKey(1004),
            hKey(1006),
            hKey(1001),
            hKey(1003),
            hKey(1005),
            hKey(1007),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    private String hKey(int id)
    {
        return String.format("{1,(long)%s}", id);
    }

    private int t;
    private TableRowType tRowType;
    private IndexRowType txIndexRowType;
    private Group groupTable;
}
