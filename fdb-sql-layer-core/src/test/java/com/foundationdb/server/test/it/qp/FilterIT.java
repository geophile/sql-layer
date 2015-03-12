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
import com.foundationdb.qp.rowtype.AisRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.foundationdb.qp.operator.API.*;

public class FilterIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        Row[] dbWithOrphans = new Row[]{
            row(customer, 1L, "northbridge"),
            row(customer, 2L, "foundation"),
            row(address, 1001L, 1L, "111 1111 st"),
            row(address, 1002L, 1L, "111 2222 st"),
            row(address, 2001L, 2L, "222 1111 st"),
            row(address, 2002L, 2L, "222 2222 st"),
            row(order, 11L, 1L, "ori"),
            row(order, 12L, 1L, "david"),
            row(order, 21L, 2L, "tom"),
            row(order, 22L, 2L, "jack"),
            row(item, 111L, 11L),
            row(item, 112L, 11L),
            row(item, 121L, 12L),
            row(item, 122L, 12L),
            row(item, 211L, 21L),
            row(item, 212L, 21L),
            row(item, 221L, 22L),
            row(item, 222L, 22L)
        };
        use(dbWithOrphans);
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testNullKeepTypes()
    {
        filter_Default(groupScan_Default(coi), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyKeepTypes()
    {
        filter_Default(groupScan_Default(coi), Collections.<RowType>emptySet());
    }

    // Test operator execution (former Cut tests)

    @Test
    public void testCutBelowCustomer()
    {
        Operator plan = filter_Default(groupScan_Default(coi), removeDescendentTypes(customerRowType));
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCutBelowOrder()
    {
        Operator plan = filter_Default(groupScan_Default(coi), removeDescendentTypes(orderRowType));
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(orderRowType, 12L, 1L, "david"),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(orderRowType, 22L, 2L, "jack"),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, "222 2222 st")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCutBelowItem()
    {
        Operator plan = filter_Default(groupScan_Default(coi), removeDescendentTypes(itemRowType));
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, "222 2222 st")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCutBelowAddress()
    {
        Operator plan = filter_Default(groupScan_Default(coi), removeDescendentTypes(addressRowType));
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, "222 2222 st")
        };
        compareRows(expected, cursor);
    }

    // Test operator execution (former extract tests)

    @Test
    public void testExtractRoot()
    {
        Operator plan = filter_Default(groupScan_Default(coi), typeAndDescendents(customerRowType));
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, "222 2222 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testExtractLeaf()
    {
        Operator plan = filter_Default(groupScan_Default(coi), typeAndDescendents(itemRowType));
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testExtractSiblings()
    {
        Set<RowType> keepTypes = new HashSet<>();
        keepTypes.addAll(typeAndDescendents(addressRowType));
        keepTypes.addAll(typeAndDescendents(orderRowType));
        Operator plan = filter_Default(groupScan_Default(coi), keepTypes);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st"),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, "222 2222 st"),
        };
        compareRows(expected, cursor);
    }

    // No test for cursor lifecycle needed -- tested in several other operator ITs, including
    // GroupScanIT.testCursor.

    private Set<TableRowType> removeDescendentTypes(AisRowType type)
    {
        Set<TableRowType> keepTypes = type.schema().userTableTypes();
        keepTypes.removeAll(Schema.descendentTypes(type, keepTypes));
        return keepTypes;
    }

    private Set<RowType> typeAndDescendents(RowType type)
    {
        Set<RowType> keepTypes= new HashSet<>();
        keepTypes.addAll(Schema.descendentTypes(type, schema.userTableTypes()));
        keepTypes.add(type);
        return keepTypes;
    }
}
