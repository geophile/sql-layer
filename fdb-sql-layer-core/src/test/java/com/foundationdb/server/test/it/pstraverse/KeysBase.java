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
package com.foundationdb.server.test.it.pstraverse;

import com.foundationdb.ais.model.Index;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.test.it.keyupdate.CollectingIndexKeyVisitor;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class KeysBase extends ITBase {
    private int customers;
    private int orders;
    private int items;

    protected abstract String ordersPK();
    protected abstract String itemsPK();

    @Before
    public void setUp() throws Exception {
        String schema = "cascading";
        customers = createTable(schema, "customers", "cid bigint not null primary key");
        orders = createTable(schema, "orders",
                "cid bigint not null",
                "oid bigint not null",
                "PRIMARY KEY("+ordersPK()+")",
                "GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)"
        );
        items = createTable(schema, "items",
                "cid bigint not null",
                "oid bigint not null",
                "iid bigint not null",
                "PRIMARY KEY("+itemsPK()+")",
                "GROUPING FOREIGN KEY ("+ordersPK()+") REFERENCES orders("+ordersPK()+")"
        );

        writeRows(
                row(customers, 71L),
                row(orders, 71L, 81L),
                row(items, 71L, 81L, 91L),
                row(items, 71L, 81L, 92L),
                row(orders, 72L, 82L),
                row(items, 72L, 82L, 93L)

        );
    }

    protected int customers() {
        return customers;
    }

    protected int orders() {
        return orders;
    }

    protected int items() {
        return items;
    }

    @Test // (expected=IllegalArgumentException.class) @SuppressWarnings("unused") // junit will invoke
    public void traverseCustomersPK() throws Exception {
        traversePK(
                customers(),
                Arrays.asList(71L)
        );
    }

    @Test @SuppressWarnings(value={"unused", "unchecked"}) // junit will invoke
    public void traverseOrdersPK() throws Exception {
        traversePK(
                orders(),
                Arrays.asList(81L, 71L),
                Arrays.asList(82L, 72L)
        );
    }

    @Test @SuppressWarnings(value={"unused", "unchecked"}) // junit will invoke
    public void traverseItemsPK() throws Exception {
        traversePK(
                items(),
                Arrays.asList(91L, 71L, 81L),
                Arrays.asList(92L, 71L, 81L),
                Arrays.asList(93L, 72L, 82L)
        );
    }

    protected void traversePK(int tableID, List<?>... expectedIndexes) throws Exception {
        Index pkIndex = getTable(tableID).getPrimaryKeyIncludingInternal().getIndex();

        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            CollectingIndexKeyVisitor visitor = new CollectingIndexKeyVisitor();
            store().traverse(session(), pkIndex, visitor, -1, 0);
            assertEquals("traversed indexes", Arrays.asList(expectedIndexes), visitor.records());
            txn.commit();
        }
    }

    @Test
    public void scanCustomers() throws InvalidOperationException {
        expectRows(
                customers,
                row(customers, 71L)
        );
    }

    @Test
    public void scanOrders() throws InvalidOperationException {
        expectRows(
                orders,
                row(orders, 71L, 81L),
                row(orders, 72L, 82L)
        );
    }

    @Test
    public void scanItems() throws InvalidOperationException {
        expectRows(
                items,
                row(items, 71L, 81L, 91L),
                row(items, 71L, 81L, 92L),
                row(items, 72L, 82L, 93L)
        );
    }
}
