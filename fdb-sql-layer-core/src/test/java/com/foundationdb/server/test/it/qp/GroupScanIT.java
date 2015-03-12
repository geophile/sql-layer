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
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.value.ValueSource;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.foundationdb.qp.operator.API.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/*
 * There are 4 usages of GroupScan_Default:
 * 1. Full scan.
 * 2. Retrieve row with hkey, without descendents.
 * 3. Retrieve row with hkey, with descendents.
 * 4. Retrieve rows satisfying range condition on hkey.
 * These correspond to the four ways in which the underlying cursor (e.g. FDBGroupCursor) can be used.
 */

public class GroupScanIT extends OperatorITBase
{
    @Test
    public void testFullScan()
    {
        use(db);
        Operator groupScan = groupScan_Default(coi);
        Cursor cursor = cursor(groupScan, queryContext, queryBindings);
        Row[] expected = new Row[]{row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(itemRowType, 112L, 11L),
                                   row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(itemRowType, 122L, 12L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testFullScan_EmptyDB()
    {
        use(emptyDB);
        Operator groupScan = groupScan_Default(coi);
        Cursor cursor = cursor(groupScan, queryContext, queryBindings);
        compareRows(EMPTY_EXPECTED, cursor);
    }

    @Test
    public void testFindHKeyWithoutDescendents()
    {
        // A group scan locating a single hkey with its descendents is done by the AncestorLookup operator.
        // Can't test it directly here since this isn't a unit test (and we don't have access to the wrapped
        // GroupCursor or GroupCursor.rebind).
        use(db);
        IndexBound tom = orderSalesmanIndexBound("tom");
        IndexKeyRange indexKeyRange = IndexKeyRange.bounded(orderSalesmanIndexRowType, tom, true, tom, true);
        Operator groupScan = indexScan_Default(orderSalesmanIndexRowType, false, indexKeyRange);
        Operator ancestorLookup = ancestorLookup_Default(groupScan,
                                                                 coi,
                                                                 orderSalesmanIndexRowType,
                                                                 Arrays.asList(customerRowType),
                                                                 InputPreservationOption.DISCARD_INPUT);
        Cursor cursor = cursor(ancestorLookup, queryContext, queryBindings);
        Row[] expected = new Row[]{row(customerRowType, 2L, "abc")};
        compareRows(expected, cursor);
    }

    @Test
    public void testFindHKeyWithoutDescendents_EmptyDB()
    {
        // Testing GroupScan via AncestorLookup doesn't work for an empty database, since there won't be
        // any uses of GroupScan.
    }

    @Test
    public void testFindHKeyWithDescendents()
    {
        // A group scan locating a single hkey with its descendents is done by the Lookup operator.
        // Can't test it directly here since this isn't a unit test (and we don't have access to the wrapped
        // GroupCursor or GroupCursor.rebind).
        use(db);
        IndexBound tom = orderSalesmanIndexBound("tom");
        IndexKeyRange indexKeyRange = IndexKeyRange.bounded(orderSalesmanIndexRowType, tom, true, tom, true);
        Operator groupScan = indexScan_Default(orderSalesmanIndexRowType, false, indexKeyRange);
        Operator lookup = branchLookup_Default(groupScan, coi, orderSalesmanIndexRowType, orderRowType, InputPreservationOption.DISCARD_INPUT  );
        Cursor cursor = cursor(lookup, queryContext, queryBindings);
        Row[] expected = new Row[]{row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testFindHKeyWithDescendents_EmptyDB()
    {
        // Testing GroupScan via Lookup doesn't work for an empty database, since there won't be
        // any uses of GroupScan.
    }
    
    // Inspired by bug 898013
    @Test
    public void testAliasingOfPersistitGroupRowFields()
    {
        use(db);
        Operator groupScan = groupScan_Default(coi);
        Cursor cursor = cursor(groupScan, queryContext, queryBindings);
        cursor.openTopLevel();
        Row row = cursor.next();
        assertSame(customerRowType, row.rowType());
        row = cursor.next();
        assertSame(orderRowType, row.rowType());
        // Get and checking each field should work
        assertEquals(Long.valueOf(11L), getLong(row, 0));
        assertEquals(Long.valueOf(1L), getLong(row, 1));
        assertEquals("ori", row.value(2).getString());
        // Getting all value sources and then using them should also work
        ValueSource v0 = row.value(0);
        ValueSource v1 = row.value(1);
        ValueSource v2 = row.value(2);
        assertEquals(11L, v0.getInt32());
        assertEquals(1L, v1.getInt32());
        assertEquals("ori", v2.getString());
    }

    @Test
    public void testCursor()
    {
        use(db);
        Operator plan =
            filter_Default(
                groupScan_Default(coi),
                Collections.singleton(customerRowType));
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(customerRowType, 1L, "xyz"),
                    row(customerRowType, 2L, "abc"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private IndexBound orderSalesmanIndexBound(String salesman)
    {
        return new IndexBound(row(orderSalesmanIndexRowType, salesman), new SetColumnSelector(0));
    }

    private IndexBound customerCidIndexBound(int cid)
    {
        return new IndexBound(row(customerCidIndexRowType, cid), new SetColumnSelector(0));
    }

    /**
     * For use in HKey scan (bound contains user table rows)
     */
    private IndexBound customerCidBound(int cid)
    {
        return new IndexBound(row(customerRowType, cid, null), new SetColumnSelector(0));
    }

    private static final Row[] EMPTY_EXPECTED = new Row[]{};
}
