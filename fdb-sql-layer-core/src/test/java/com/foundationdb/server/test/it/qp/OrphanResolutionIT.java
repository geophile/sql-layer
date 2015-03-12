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
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;

import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.qp.operator.API.*;

public class OrphanResolutionIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null",
            "px int",
            "primary key(pid)");
        child = createTable(
            "schema", "child",
            "pid int",
            "cx int",
            "grouping foreign key(pid) references parent(pid)");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        group = group(parent);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[] {
            row(child, 1L, 100L),
            row(child, 1L, 101L),
        };
        use(db);
    }

    // Inspired by bug 1020342.

    @Test
    public void test()
    {
        Operator insertPlan = insert_Returning(valuesScan_Default(Arrays.asList(parentRow(1, 10)), parentRowType));
        runPlan(queryContext, queryBindings, insertPlan);
        // Execution of insertPlan used to hang before 1020342 was fixed.
        Row[] expected = new Row[] {
            row(parentRowType, 1L, 10L),
            // Last column of child rows is generated PK value
            row(childRowType, 1L, 100L, 1L),
            row(childRowType, 1L, 101L, 2L),
        };
        compareRows(expected, cursor(groupScan_Default(group), queryContext, queryBindings));
    }

    private BindableRow parentRow(int pid, int px)
    {
        return BindableRow.of(parentRowType, Arrays.asList(ExpressionGenerators.literal(pid, MNumeric.INT.instance(true)),
                                                           ExpressionGenerators.literal(px, MNumeric.INT.instance(true))));
    }

    private int parent;
    private int child;
    private TableRowType parentRowType;
    private TableRowType childRowType;
    private Group group;
}
