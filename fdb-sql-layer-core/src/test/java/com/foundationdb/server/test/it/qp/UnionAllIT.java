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

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.select_HKeyOrdered;
import static com.foundationdb.qp.operator.API.unionAll_Default;
import static com.foundationdb.qp.operator.API.union_Ordered;

import com.foundationdb.qp.row.Row;

import org.junit.Test;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.SetWrongTypeColumns;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.texpressions.Comparison;

public class UnionAllIT extends OperatorITBase {

    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "primary key(id)");
        createIndex("schema", "t", "tx", "x");
        u = createTable (
                "schema", "u",
                "id int not null primary key",
                "x int");
        v = createTable (
             "schema", "v",
             "id int not null primary key",
             "name varchar(32)");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = SchemaCache.globalSchema(ais());
        tRowType = schema.tableRowType(table(t));
        uRowType = schema.tableRowType(table(u));
        vRowType = schema.tableRowType(table(v));
        tGroupTable = group(t);
        uGroupTable = group(u);
        vGroupTable = group(v);
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
            row(u, 1000L, 7L),
            row(u, 1001L, 9L),
            row(u, 1002L, 9L),
            row(u, 1003L, 9L)
        };
        use(db);
    }
    private int t, u, v;
    private TableRowType tRowType, uRowType, vRowType;
    private Group tGroupTable, uGroupTable, vGroupTable;
    
    @Test
    public void testBothNonEmpty()
    {
        Operator plan =
            unionAll_Default(
                select_HKeyOrdered(
                    groupScan_Default(tGroupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(8), castResolver())),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(uGroupTable),
                    uRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(uRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(7), castResolver())),
                uRowType, 
                true);
        Row[] expected = new Row[]{
            row(tRowType, 1000L, 8L),
            row(tRowType, 1002L, 8L),
            row(tRowType, 1004L, 8L),
            row(tRowType, 1006L, 8L),
            row(uRowType, 1000L, 7L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    
    @Test
    public void testOrderedNonEmpty()
    {
        Operator plan =
            union_Ordered(
                select_HKeyOrdered(
                    groupScan_Default(tGroupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                select_HKeyOrdered(
                    groupScan_Default(uGroupTable),
                    uRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(uRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                tRowType,
                uRowType, 
                2,
                2,
                ascending (true, true),
                false);
        Row[] expected = new Row[]{
            row(tRowType, 1001L, 9L),
            row(uRowType, 1002L, 9L),
            row(tRowType, 1003L, 9L),
            row(tRowType, 1005L, 9L),
            row(tRowType, 1007L, 9L)
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    
    @Test(expected = SetWrongTypeColumns.class)
    public void testAllRowTypeMismatch () {
        unionAll_Default (
                groupScan_Default(tGroupTable),
                tRowType,
                groupScan_Default(vGroupTable),
                vRowType,
                true);
    }
    
    @Test (expected = SetWrongTypeColumns.class)
    public void testDifferentInputTypes() 
    {
        // Test different input types
        union_Ordered(groupScan_Default(tGroupTable),
                      groupScan_Default(vGroupTable),
                      tRowType,
                      vRowType,
                      2,
                      2,
                      ascending(true,true),
                      false);
    }

    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
    
}
