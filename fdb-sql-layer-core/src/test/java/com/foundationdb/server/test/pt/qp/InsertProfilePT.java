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
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.InvalidOperationException;

import org.junit.Before;
import org.junit.Test;

public class InsertProfilePT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        customer = createTable(
            "schema", "customer",
            "cid int not null key",
            "name varchar(20)," +
            "index(name)");
        order = createTable(
            "schema", "order",
            "oid int not null key",
            "cid int",
            "salesman varchar(20)",
            "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)",
            "index(salesman)");
        item = createTable(
            "schema", "item",
            "iid int not null key",
            "oid int",
            "constraint __akiban_io foreign key __akiban_io(oid) references order(oid)");
        address = createTable(
            "schema", "address",
            "aid int not null key",
            "cid int",
            "address varchar(100)",
            "constraint __akiban_ac foreign key __akiban_ac(cid) references customer(cid)",
            "index(address)");
        coi = group(customer);
        TableName groupName = coi.getName();
        createLeftGroupIndex(groupName, "name_salesman", "customer.name", "order.salesman");
        createLeftGroupIndex(groupName, "name_address", "customer.name", "address.address");
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
        adapter = newStoreAdapter();
    }

    @Test
    public void profileGroupScan()
    {
        final int CUSTOMERS = 100000000;
        final int ORDERS_PER_CUSTOMER = 5;
        final int ITEMS_PER_ORDER = 2;
        populateDB(CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER);
    }

    private void populateDB(int customers, int ordersPerCustomer, int itemsPerOrder)
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

    private int customer;
    private int order;
    private int item;
    private int address;
    private Group coi;
    private Schema schema;
    private RowType      customerRowType;
    private RowType orderRowType;
    private RowType itemRowType;
    private RowType addressRowType;
    private IndexRowType customerNameIndexRowType;
    private IndexRowType orderSalesmanIndexRowType;
    private IndexRowType itemOidIndexRowType;
    private IndexRowType itemIidIndexRowType;
    private IndexRowType customerCidIndexRowType;
    private IndexRowType addressAddressIndexRowType;
    private StoreAdapter adapter;
}
