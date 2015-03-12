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
package com.foundationdb.server.test.it.dxl;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.util.Exceptions;
import org.junit.Test;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.ITBase;

// Inspired by bug 1078331

public class WriteSkewIT extends ITBase
{
    // Test case from bug 1078331, comment 2
    @Test
    public void testHKeyMaintenance() throws InterruptedException
    {
        createDatabase();
        writeRow(parent, 1, 100);
        writeRow(parent, 2, 200);
        writeRow(child, 1, 1, 1100);
        writeRow(grandchild, 1, 1, 11100);
        TestThread threadA = new TestThread("a")
        {
            @Override
            public void doAction()
            {
                writeRow(threadPrivateSession, child, 2, 2, 2200);
            }
        };
        TestThread threadB = new TestThread("b")
        {
            @Override
            public void doAction()
            {
                writeRow(threadPrivateSession, grandchild, 2, 2, 22200);
            }
        };
        runTest(
            threadA,
            threadB,
            new Runnable() {
                @Override
                public void run() {
                    expectRows(
                        getTable(parent).getGroup(),
                        row(parent, 1, 100),
                        row(child, 1, 1, 1100),
                        row(grandchild, 1, 1, 11100),
                        row(parent, 2, 200),
                        row(child, 2, 2, 2200),
                        row(grandchild, 2, 2, 22200)
                    );
                }
            }
        );
    }

    // Test case from description of bug 1078331
    @Test
    public void testGroupIndexMaintenance() throws InterruptedException
    {
        createDatabase();
        writeRow(parent, 1, 100);
        writeRow(child, 11, 1, 1100);
        TestThread threadA = new TestThread("a")
        {
            @Override
            public void doAction()
            {
                writeRow(threadPrivateSession, parent, 2, 2200);
            }
        };
        TestThread threadB = new TestThread("b")
        {
            @Override
            public void doAction()
            {
                updateRow(threadPrivateSession,
                          row(threadPrivateSession, child, 11, 1, 1100),
                          row(threadPrivateSession, child, 11, 2, 1100));
            }
        };
        runTest(
            threadA,
            threadB,
            new Runnable() {
                @Override
                public void run() {
                    GroupIndex index = getTable(parent).getGroup().getIndex(groupIndexName);
                    expectRows(
                        index,
                        row(index, 100, null, 1, null),
                        row(index, 2200, 1100, 2, 11)
                    );
                }
            }
        );
    }

    private void runTest(TestThread sessionA, TestThread sessionB, Runnable bothPassedCheck) throws InterruptedException
    {
        sessionA.start();
        sessionB.start();

        sessionA.semA.release();
        sessionA.semB.tryAcquire(5, TimeUnit.SECONDS);
        
        sessionB.semA.release();
        /**
         * Give session B time to conflict before A
         * commits.
         */
        sessionB.semB.tryAcquire(1, TimeUnit.SECONDS);
        
        sessionA.semA.release();
        sessionA.semB.tryAcquire(5, TimeUnit.SECONDS);
                
        sessionA.join(10000);
        sessionB.join(10000);
        assertNull(sessionA.termination());
        if(sessionB.termination != null) {
            assertTrue("sessionB termination was rollback", Exceptions.isRollbackException(sessionB.termination()));
        } else {
            bothPassedCheck.run();
        }
    }

    private void createDatabase() throws InvalidOperationException
    {
        final String SCHEMA = "schema";
        parent = createTable(SCHEMA, "parent",
                             "pid int not null",
                             "x int",
                             "primary key(pid)");
        child = createTable(SCHEMA, "child",
                            "cid int not null",
                            "pid int",
                            "y int",
                            "primary key(cid)",
                            "grouping foreign key(pid) references parent(pid)");
        grandchild = createTable(SCHEMA, "grandchild",
                                 "gid int not null",
                                 "cid int",
                                 "z int",
                                 "primary key(gid)",
                                 "grouping foreign key(cid) references child(cid)");
        createLeftGroupIndex(TableName.create(SCHEMA, "parent"), groupIndexName, "parent.x", "child.y");
    }

    private int parent;
    private int child;
    private int grandchild;

    private final AtomicBoolean exceptionInAnyThread = new AtomicBoolean(false);
    private final String groupIndexName = "idx_pxcy";
    
    abstract private class TestThread extends Thread
    {
        
        TestThread(final String name) {
            super(name);
        }
        
        @Override
        public void run()
        {
            txnService().beginTransaction(threadPrivateSession);
            boolean committed = false;
            try {
                semA.tryAcquire(5, TimeUnit.SECONDS);
                doAction();
                semB.release();
                semA.tryAcquire(5, TimeUnit.SECONDS);
                txnService().commitTransaction(threadPrivateSession);
                semB.release();
                committed = true;
            } catch (Exception e) {
                termination = e;
                exceptionInAnyThread.set(true);
            } finally {
                if (!committed) {
                    txnService().rollbackTransactionIfOpen(threadPrivateSession);
                    semB.release();
                }
            }
        }
        
        abstract protected void doAction();

        public Exception termination()
        {
            return termination;
        }

        protected final Session threadPrivateSession = serviceManager().getSessionService().createSession();
        private Exception termination;
        private final Semaphore semA = new Semaphore(0);
        private final Semaphore semB = new Semaphore(0);
    }

}
