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

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.QueryTimedOutException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TransactionServiceIT extends ITBase
{
    private int tid = -1;

    @Before
    public void setupTable() {
        tid = createTable("test", "t", "id int not null primary key");
    }

    @Test
    public void runnableSuccess() {
        final Row row = row(tid, 1);
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                writeRows(row);
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void runnableFailure() {
        final RuntimeException ex = new RuntimeException();
        try {
            txnService().run(session(), new Runnable() {
                @Override
                public void run() {
                    writeRow(tid, 1);
                    throw ex;
                }
            });
            fail("Expected exception");
        } catch(RuntimeException e) {
            assertEquals(ex, e);
        }
        expectRowCount(tid, 0);
    }

    @Test
    public void singleRunnableRetry() {
        final Row row = row(tid, 1);
        final int[] failures = { 0 };
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                writeRows(row);
                if(failures[0] < 1) {
                    ++failures[0];
                    throw new QueryTimedOutException(-1);
                }
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void multipleRunnableRetry() {
        final Row row = row(tid, 1);
        final int failures[] = { 0 };
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                writeRows(row);
                if(failures[0] < 2) {
                    ++failures[0];
                    throw new QueryTimedOutException(-1);
                }
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void callableSuccess() {
        Row row = txnService().run(session(), new Callable<Row>() {
            @Override
            public Row call() {
                Row row = row(tid, 1);
                writeRows(row);
                return row;
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void callableFailure() {
        final RuntimeException ex = new RuntimeException();
        try {
            txnService().run(session(), new Callable<Row>() {
                @Override
                public Row call() {
                    writeRow(tid, 1);
                    throw ex;
                }
            });
            fail("Expected exception");
        } catch(RuntimeException e) {
            assertEquals(ex, e);
        }
        expectRowCount(tid, 0);
    }

    @Test(expected=AkibanInternalException.class)
    public void callableThrowsException() {
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                throw new Exception();
            }
        });
    }

    @Test
    public void singleCallableRetry() {
        final int[] failures = { 0 };
        Row row = txnService().run(session(), new Callable<Row>() {
            @Override
            public Row call() {
                Row row = row(tid, 1);
                writeRows(row);
                if(failures[0] < 1) {
                    ++failures[0];
                    throw new QueryTimedOutException(-1);
                }
                return row;
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void multipleCallableRetry() {
        final int[] failures = { 0 };
        Row row = txnService().run(session(), new Callable<Row>() {
            @Override
            public Row call() {
                Row row = row(tid, 1);
                writeRows(row);
                if(failures[0] < 2) {
                    ++failures[0];
                    throw new QueryTimedOutException(-1);
                }
                return row;
            }
        });
        expectRows(tid, row);
    }
}
