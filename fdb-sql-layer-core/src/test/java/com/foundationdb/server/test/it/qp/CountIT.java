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

import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import org.junit.Test;

import java.util.Collections;

import static com.foundationdb.qp.operator.API.*;

public class CountIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        Row[] dbRows = new Row[]{
            row(customer, 1L, "northbridge"),
            row(customer, 2L, "foundation"),
            row(customer, 4L, "highland"),
            row(order, 11L, 1L, "ori")
        };
        use(dbRows);
    }

    // Count tests

    @Test
    public void testCustomerCid()
    {
        Operator plan = count_Default(groupScan_Default(coi),
                                              customerRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        RowType resultRowType = plan.rowType();
        Row[] expected = new Row[]{
            row(orderRowType, 11L, 1L, "ori"),
            row(resultRowType, 3L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCustomers()
    {
        Operator plan = count_TableStatus(customerRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        RowType resultRowType = plan.rowType();
        Row[] expected = new Row[]{
            row(resultRowType, 3L)
        };
        compareRows(expected, cursor);
        writeRows(row(customer, 5L, "matrix"));
        expected = new Row[]{
            row(resultRowType, 4L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCount_DefaultCursor()
    {
        Operator plan =
            count_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                customerRowType);
        final RowType resultRowType = plan.rowType();
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(resultRowType, 3L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test
    public void testCount_TableStatusCursor()
    {
        Operator plan = count_TableStatus(customerRowType);
        final RowType resultRowType = plan.rowType();
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(resultRowType, 3L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }
}
