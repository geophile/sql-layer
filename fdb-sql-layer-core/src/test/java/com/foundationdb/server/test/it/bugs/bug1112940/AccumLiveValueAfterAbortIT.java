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
package com.foundationdb.server.test.it.bugs.bug1112940;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.TableStatus;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * The cost estimator uses the live value from the row count accumulator
 * as it is much cheaper than a (transactional) snapshot value. However, the
 * live value is not adjusted if a modification is aborted.
 *
 * For no particular reason, the row count was bumped before we did any
 * uniqueness checks in the write row processing. This caused an increasingly
 * inaccurate gap for replication streams that use ON DUPLICATION UPDATE
 * processing, which the server sees as INSERT, (error), UPDATE
 */
public class AccumLiveValueAfterAbortIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final int ROW_COUNT = 2;
    private static final int UPDATE_COUNT = 10;
    private static enum Op { ON_DUP_KEY_UPDATE, REPLACE }

    private int tid = -1;

    private void createAndLoad() {
        tid = createTable(SCHEMA, TABLE, "id int not null primary key, x int");
        for(int i = 0; i < ROW_COUNT; ++i) {
            writeRow(tid, i, 0);
        }
        expectRowCount(tid, ROW_COUNT);
    }

    private void postCheck() {
        expectRowCount(tid, ROW_COUNT);
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                // Approximate count doesn't have to match in general, but there
                // is no reason for it to be off in these simple scenarios
                TableStatus tableStatus = getTable(tid).tableStatus();
                assertEquals("ApproximateRowCount", ROW_COUNT, tableStatus.getApproximateRowCount(session()));
            }
        });
    }

    private void insertAs(Op op, int id, int x) {
        Row newRow = row(tid, id, x);
        try {
            writeRow(newRow);
            fail("Expected DuplicateKeyException");
        } catch(DuplicateKeyException e) {
            // Expected
        }

        Row oldRow = row(tid, id, x - 1);
        if(op == Op.ON_DUP_KEY_UPDATE) {
            updateRow(oldRow, newRow);
        } else if(op == Op.REPLACE) {
            deleteRow(oldRow);
            writeRow(newRow);
        } else {
            fail("Unknown op: " + op);
        }
    }

    private void testOp(Op op) {
        createAndLoad();
        final int UPDATE_ID = 1;
        for(int i = 1; i <= UPDATE_COUNT; ++i) {
            insertAs(op, UPDATE_ID, i);
        }
        postCheck();
    }

    @Test
    public void onDuplicateKeyUpdate() {
        testOp(Op.ON_DUP_KEY_UPDATE);
    }

    @Test
    public void replace() {
        testOp(Op.REPLACE);
    }
}
