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
import com.foundationdb.server.error.NegativeLimitException;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;

public class LimitIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
        Row[] dbRows = new Row[]{
            row(customer, 1L, "northbridge"),
            row(customer, 2L, "foundation"),
            row(customer, 4L, "highland"),
            row(customer, 5L, "matrix"),
            row(customer, 6L, "sigma"),
            row(customer, 7L, "crv"),
        };
        use(dbRows);
    }

    // Limit tests

    @Test
    public void testLimit()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              3);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 4L, "highland"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSkip()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              2, false, Integer.MAX_VALUE, false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
            row(customerRowType, 6L, "sigma"),
            row(customerRowType, 7L, "crv"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSkipAndLimit()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              2, false, 2, false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSkipExhausted()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              10, false, 1, false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testLimitFromBinding()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              0, false, 0, true);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        queryBindings.setValue(0, new Value(MNumeric.INT.instance(false), 2));
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
        };
        compareRows(expected, cursor);
    }

    @Test(expected = NegativeLimitException.class)
    public void testLimitFromBadBinding()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              0, false, 0, true);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        queryBindings.setValue(0, new Value(MNumeric.INT.instance(false), -1));
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor);
    }

    @Test(expected=NegativeLimitException.class)
    public void testOffsetFromBadBinding () 
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                0, true, 0, false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        queryBindings.setValue(0, new Value(MNumeric.INT.instance(false), -1));
        Row[] expected = new Row[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        Operator plan = limit_Default(groupScan_Default(coi), 3);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(customerRowType, 1L, "northbridge"),
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 4L, "highland"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }
}
