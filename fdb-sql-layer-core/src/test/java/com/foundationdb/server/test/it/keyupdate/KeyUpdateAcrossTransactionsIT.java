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
package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.IndexKeyVisitor;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

// Inspired by bug 985007

@Ignore
public final class KeyUpdateAcrossTransactionsIT extends ITBase
{
    @Before
    public final void before() throws Exception
    {
        testStore = new TestStore(store());
    }

    @Test
    public void testUniqueViolationAcrossTransactions() throws Exception
    {
        final String TABLE_NAME = "t";
        final String SCHEMA_NAME = "s";
        final int tableId=
            createTable(SCHEMA_NAME, TABLE_NAME,
                        "id int not null primary key",
                        "u int",
                        "unique(u)");
        writeRows(row(tableId, 0, 0));
        AkibanInformationSchema ais = ddl().getAIS(session());
        Table table = ais.getTable(tableId);
        Index uIndex = null;
        for (TableIndex index : table.getIndexes()) {
            if (index.getKeyColumns().get(0).getColumn().getName().equals("u")) {
                uIndex = index;
            }
        }
        assertNotNull(uIndex);
        final Index finalUIndex = uIndex;
        CyclicBarrier barrier = new CyclicBarrier(2);
        TestThread t1 = createThread(barrier, tableId, 100, 999);
        TestThread t2 = createThread(barrier, tableId, 101, 999);
        t1.join();
        t2.join();
        final Set<Long> uniqueKeys = new HashSet<>();
        transactionally(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                testStore.traverse(session(),
                                   finalUIndex,
                                   new IndexKeyVisitor()
                                   {
                                       @Override
                                       protected void visit(List<?> key)
                                       {
                                           Long u = (Long) key.get(0);
                                           boolean added = uniqueKeys.add(u);
                                           assertTrue(key.toString(), added);
                                       }
                                   }, 
                                   -1, 0);
                return null;
            }
        });
    }

    public TestThread createThread(CyclicBarrier barrier, int tableId, int id, int u)
    {
        TestThread thread = new TestThread(barrier, tableId, id, u);
        thread.start();
        return thread;
    }

    private TestStore testStore;

    private class TestThread extends Thread
    {
        @Override
        public void run()
        {
            try {
                Session session = createNewSession();
                txnService().beginTransaction(session);
                try {
                    System.out.println(String.format("(%s, %s), %s: Starting", id, u, session));
                    barrier.await();
                    System.out.println(String.format("(%s, %s), %s: About to write", id, u, session));
                    writeRows(session, Arrays.asList(row(session, tableId, id, u)));
                    System.out.println(String.format("(%s, %s), %s: Wrote", id, u, session));
                    barrier.await();
                    System.out.println(String.format("(%s, %s), %s: Exiting", id, u, session));
                    txnService().commitTransaction(session);
                }
                finally {
                    txnService().rollbackTransactionIfOpen(session);
                }
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }

        public TestThread(CyclicBarrier barrier, int tableId, int id, int u)
        {
            this.barrier = barrier;
            this.tableId = tableId;
            this.id = id;
            this.u = u;
        }

        private final CyclicBarrier barrier;
        private final int tableId;
        private final int id;
        private final int u;
    }
}
