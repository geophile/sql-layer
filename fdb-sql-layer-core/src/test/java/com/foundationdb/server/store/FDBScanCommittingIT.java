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
package com.foundationdb.server.store;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.qp.storeadapter.FDBGroupCursor;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.server.test.it.qp.TestRow;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

public class FDBScanCommittingIT extends FDBITBase
{
    private static final String SCHEMA = "test";

    private Group group;
    private FDBAdapter adapter;
    private Row[] expected;

    static final int NT1 = 5, NT2 = 10;

    @Before
    public void populate() {
        createFromDDL(SCHEMA,
                      "CREATE TABLE t1(id INT PRIMARY KEY, name VARCHAR(16));\n" +
                      "CREATE TABLE t2(id INT PRIMARY KEY, pid INT, GROUPING FOREIGN KEY(pid) REFERENCES t1(id), name VARCHAR(16));");
        int tid1 = ddl().getTableId(session(), new TableName(SCHEMA, "t1"));
        int tid2 = ddl().getTableId(session(), new TableName(SCHEMA, "t2"));

        Table t1 = getTable(tid1);
        Table t2 = getTable(tid2);
        group = t1.getGroup();
        
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType t1Type = schema.tableRowType(t1);
        RowType t2Type = schema.tableRowType(t2);
        adapter = (FDBAdapter)newStoreAdapter();

        txnService().beginTransaction(session());
        
        List<Row> inserted = new ArrayList<>(NT1 * NT2);
        
        for (int i1 = 0; i1 < NT1; i1++) {
            Object[] r1 = { i1, Integer.toString(i1) };
            writeRow(tid1, r1);
            inserted.add(new TestRow(t1Type, r1));
            for (int i2 = 0; i2 < NT2; i2++) {
                Object[] r2 = { i1 * 1000 + i2, i1, String.format("%d-%d", i1, i2) };
                writeRow(tid2, r2);
                inserted.add(new TestRow(t2Type, r2));
            }
        }
        
        expected = inserted.toArray(new Row[inserted.size()]);

        txnService().commitTransaction(session());
    }

    protected GroupCursor committingGroupCursor(FDBScanTransactionOptions transactionOptions) {
        return new FDBGroupCursor(adapter, group, transactionOptions);
    }

    @Test
    public void checkPopulated() {
        txnService().beginTransaction(session());
        compareRows(expected, adapter.newGroupCursor(group));
        txnService().commitTransaction(session());
    }

    @Test
    public void testDumpCursor() {
        txnService().beginTransaction(session());
        compareRows(expected, adapter.newDumpGroupCursor(group, 1));
        txnService().commitTransaction(session());
    }

    @Test
    public void testSnapshot() {
        txnService().beginTransaction(session());
        compareRows(expected, committingGroupCursor(new FDBScanTransactionOptions(true)));
        txnService().commitTransaction(session());
    }

    @Test
    public void testSeveralRows() {
        txnService().beginTransaction(session());
        compareRows(expected, committingGroupCursor(new FDBScanTransactionOptions(2, -1)));
        txnService().commitTransaction(session());
    }

    @Test
    public void testShortTime() {
        txnService().beginTransaction(session());
        compareRows(expected, committingGroupCursor(new FDBScanTransactionOptions(-1, 2)));
        txnService().commitTransaction(session());
    }

    @Test
    public void testMultipleCursors() {
        txnService().beginTransaction(session());
        GroupCursor c1 = committingGroupCursor(new FDBScanTransactionOptions(1, -1));
        c1.open();
        GroupCursor c2 = committingGroupCursor(new FDBScanTransactionOptions(10, -1));
        c2.open();
        for (int i = 0; i < expected.length; i++) {
            compareTwoRows(expected[i], c1.next(), i);
            compareTwoRows(expected[i], c2.next(), i);
        }
        assertNull(c2.next());
        c2.close();
        assertNull(c1.next());
        c1.close();
        txnService().commitTransaction(session());
    }

}
