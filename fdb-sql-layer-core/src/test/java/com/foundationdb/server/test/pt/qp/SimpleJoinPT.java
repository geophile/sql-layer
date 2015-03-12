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
package com.foundationdb.server.test.pt.qp;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.qp.operator.API.*;

@Ignore
public class SimpleJoinPT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        customer = createTable(
            "schema", "customer",
            "cid int not null",
            "name varchar(20)",
            "primary key(cid)");
        order = createTable(
            "schema", "orders",
            "oid int not null",
            "cid int",
            "salesman varchar(20)",
            "primary key(oid)",
            "grouping foreign key (cid) references customer(cid)");
        item = createTable(
            "schema", "item",
            "iid int not null",
            "oid int",
            "primary key(iid)",
            "grouping foreign key (oid) references orders(oid)");
        address = createTable(
            "schema", "address",
            "aid int not null",
            "cid int",
            "address varchar(100)",
            "primary key(aid)",
            "grouping foreign key (cid) references customer(cid)");
        createIndex("schema", "customer", "idx_cname", "name");
        createIndex("schema", "orders", "idx_osalesman", "salesman");
        createIndex("schema", "address", "idx_aaddress", "address");
        schema = SchemaCache.globalSchema(ais());
        customerRowType = schema.tableRowType(table(customer));
        orderRowType = schema.tableRowType(table(order));
        itemRowType = schema.tableRowType(table(item));
        addressRowType = schema.tableRowType(table(address));
        customerNameIndexRowType = indexType(customer, "name");
        orderSalesmanIndexRowType = indexType(order, "salesman");
        itemOidIndexRowType = indexType(item, "oid");
        itemIidIndexRowType = indexType(item, "iid");
        customerCidIndexRowType = indexType(customer, "cid");
        addressAddressIndexRowType = indexType(address, "address");
        coi = group(customer);
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB(int customers, int ordersPerCustomer, int itemsPerOrder)
    {
        long cid = 0;
        long oid = 0;
        long iid = 0;
        for (int c = 0; c < customers; c++) {
            writeRow(customer, cid, String.format("customer %s", cid));
            for (int o = 0; o < ordersPerCustomer; o++) {
                writeRow(order, oid, cid, String.format("salesman %s", oid));
                for (int i = 0; i < itemsPerOrder; i++) {
                    writeRow(item, iid, oid);
                    iid++;
                }
                oid++;
            }
            cid++;
        }
    }

    @Test
    public void profileGroupScan()
    {
        final int SCANS = 100000000;
        final int CUSTOMERS = 1000;
        final int ORDERS_PER_CUSTOMER = 5;
        final int ITEMS_PER_ORDER = 2;
        populateDB(CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER);
        Operator plan = 
            flatten_HKeyOrdered(
                ancestorLookup_Default(
                    indexScan_Default(itemIidIndexRowType),
                    coi,
                    itemIidIndexRowType,
                    Arrays.asList(itemRowType, orderRowType),
                    InputPreservationOption.DISCARD_INPUT),
                orderRowType,
                itemRowType,
                JoinType.INNER_JOIN);
        Tap.setEnabled(".*", true);
        for (int s = 0; s < SCANS; s++) {
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null) {
            }
            cursor.closeTopLevel();
        }
        TapReport[] reports = Tap.getReport(".*");
        for (TapReport report : reports) {
            System.out.println(report);
        }
    }

    protected int customer;
    protected int order;
    protected int item;
    protected int address;
    protected TableRowType customerRowType;
    protected TableRowType orderRowType;
    protected TableRowType itemRowType;
    protected TableRowType addressRowType;
    protected IndexRowType customerCidIndexRowType;
    protected IndexRowType customerNameIndexRowType;
    protected IndexRowType orderSalesmanIndexRowType;
    protected IndexRowType itemOidIndexRowType;
    protected IndexRowType itemIidIndexRowType;
    protected IndexRowType addressAddressIndexRowType;
    protected Group coi;
}
