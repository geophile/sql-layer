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
import com.foundationdb.server.api.dml.SetColumnSelector;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.foundationdb.qp.operator.API.ancestorLookup_Default;
import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.ifEmpty_DefaultTest;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static com.foundationdb.server.test.ExpressionGenerators.literal;

public class IfEmptyIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        Row[] db = new Row[]{
            row(customer, 0L, "matrix"), // no orders
            row(customer, 2L, "foundation"), // two orders
            row(order, 200L, 2L, "david"),
            row(order, 201L, 2L, "david"),
        };
        use(db);
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testInputNull()
    {
        ifEmpty_DefaultTest(null, customerRowType, Collections.<ExpressionGenerator>emptyList(), API.InputPreservationOption.KEEP_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRowTypeNull()
    {
        ifEmpty_DefaultTest(groupScan_Default(coi), null, Collections.<ExpressionGenerator>emptyList(), API.InputPreservationOption.KEEP_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExprsNull()
    {
        ifEmpty_DefaultTest(groupScan_Default(coi), customerRowType, null, API.InputPreservationOption.KEEP_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInputPreservationNull()
    {
        ifEmpty_DefaultTest(groupScan_Default(coi), customerRowType, Collections.<ExpressionGenerator>emptyList(), null);
    }

    // Test operator execution

    @Test
    public void testNonEmptyKeepInput()
    {
        Operator plan =
            ifEmpty_DefaultTest(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(2), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                    API.InputPreservationOption.DISCARD_INPUT),
                orderRowType, Arrays.asList(literal(999), literal(999), literal("herman")),
                API.InputPreservationOption.KEEP_INPUT);
        Row[] expected = new Row[]{
            row(orderRowType, 200L, 2L, "david"),
            row(orderRowType, 201L, 2L, "david"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testNonEmptyDiscardInput()
    {
        Operator plan =
            ifEmpty_DefaultTest(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(2), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                    API.InputPreservationOption.DISCARD_INPUT),
                orderRowType, Arrays.asList(literal(999), literal(999), literal("herman")),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testEmptyKeepInput()
    {
        Operator plan =
            ifEmpty_DefaultTest(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(0), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                API.InputPreservationOption.DISCARD_INPUT),
                orderRowType, Arrays.asList(literal(999), literal(999), literal("herman")),
                API.InputPreservationOption.KEEP_INPUT);
        Row[] expected = new Row[]{
            row(orderRowType, 999L, 999L, "herman"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testEmptyDiscardInput()
    {
        Operator plan =
            ifEmpty_DefaultTest(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(0), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                API.InputPreservationOption.DISCARD_INPUT),
                orderRowType, Arrays.asList(literal(999), literal(999), literal("herman")),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[]{
            row(orderRowType, 999L, 999L, "herman"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCursorNonEmptyDefaultKeepInput()
    {
        Operator plan =
            ifEmpty_DefaultTest(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(2), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                API.InputPreservationOption.DISCARD_INPUT),
                orderRowType,
                Arrays.asList(literal(999), literal(999), literal("herman")),
                API.InputPreservationOption.KEEP_INPUT);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(orderRowType, 200L, 2L, "david"),
                    row(orderRowType, 201L, 2L, "david"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test
    public void testCursorNonEmptyDefaultDiscardInput()
    {
        Operator plan =
            ifEmpty_DefaultTest(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(2), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                API.InputPreservationOption.DISCARD_INPUT),
                orderRowType,
                Arrays.asList(literal(999), literal(999), literal("herman")),
                API.InputPreservationOption.DISCARD_INPUT);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test
    public void testCursorEmptyKeepInput()
    {
        Operator plan =
            ifEmpty_DefaultTest(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(0), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                    API.InputPreservationOption.DISCARD_INPUT),
                orderRowType,
                Arrays.asList(literal(999), literal(999), literal("herman")),
                API.InputPreservationOption.KEEP_INPUT);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(orderRowType, 999L, 999L, "herman"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test
    public void testCursorEmptyDiscardInput()
    {
        Operator plan =
            ifEmpty_DefaultTest(
                ancestorLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidKeyRange(0), asc()),
                    coi,
                    orderCidIndexRowType,
                    Collections.singleton(orderRowType),
                    API.InputPreservationOption.DISCARD_INPUT),
                orderRowType,
                Arrays.asList(literal(999), literal(999), literal("herman")),
                API.InputPreservationOption.DISCARD_INPUT);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(orderRowType, 999L, 999L, "herman"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private IndexKeyRange cidKeyRange(int cid)
    {
        IndexBound bound = new IndexBound(row(orderCidIndexRowType, cid), new SetColumnSelector(0));
        return IndexKeyRange.bounded(orderCidIndexRowType, bound, true, bound, true);
    }

    private API.Ordering asc()
    {
        API.Ordering ordering = new API.Ordering();
        ordering.append(field(orderCidIndexRowType, 0), true);
        return ordering;
    }
}
