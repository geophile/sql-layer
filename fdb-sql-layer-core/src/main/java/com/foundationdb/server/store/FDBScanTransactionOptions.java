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

/**
 * Control how a scan of an index / group interacts with transactions.
 */
public class FDBScanTransactionOptions
{
    public static final FDBScanTransactionOptions NORMAL = new FDBScanTransactionOptions();
    public static final FDBScanTransactionOptions SNAPSHOT = new FDBScanTransactionOptions(true);
   
    private final boolean snapshot;
    private final int commitAfterRows;
    private final long commitAfterMillis;
    private final long sleepAfterCommit;

    public FDBScanTransactionOptions() {
        this(false, -1, -1, -1);
    }

    public FDBScanTransactionOptions(boolean snapshot) {
        this(snapshot, -1, -1, -1);
    }

    public FDBScanTransactionOptions(int commitAfterRows, long commitAfterMillis) {
        this(false, commitAfterRows, commitAfterMillis, -1);
    }

    public FDBScanTransactionOptions(long commitAfterMillis, long sleepAfterCommit) {
        this(false, -1, commitAfterMillis, sleepAfterCommit);
    }

    public FDBScanTransactionOptions(boolean snapshot, int commitAfterRows,
                                     long commitAfterMillis, long sleepAfterCommit) {
        this.snapshot = snapshot;
        this.commitAfterRows = commitAfterRows;
        this.commitAfterMillis = commitAfterMillis;
        this.sleepAfterCommit = sleepAfterCommit;
    }

    /** Should scan use snapshot read to avoid generating conflicts? */
    public boolean isSnapshot() {
        return snapshot;
    }

    /** Should we ever commit in the middle of a scan? */
    public boolean isCommitting() {
        return ((commitAfterRows > 0) ||
                (commitAfterMillis > 0));
    }

    /** Should scan commit after a number of rows have been traversed? */
    public int getCommitAfterRows() {
        return commitAfterRows;
    }

    public boolean shouldCommitAfterRows(int rows) {
        return ((commitAfterRows > 0) &&
                (rows >= commitAfterRows));
    }
    
    /** Should scan commit after some time has passed since the
     * beginning of the transaction? */
    public long getCommitAfterMillis() {
        return commitAfterMillis;
    }

    public boolean shouldCommitAfterMillis(long startTime) {
        if (commitAfterMillis <= 0) return false;
        long dt = System.currentTimeMillis() - startTime;
        return (dt >= commitAfterMillis);
    }

    /** Time to wait after committing while scanning. */
    public long getSleepAfterCommit() {
        return sleepAfterCommit;
    }

    public void maybeSleepAfterCommit() throws InterruptedException {
        if (sleepAfterCommit > 0) {
            Thread.sleep(sleepAfterCommit);
        }
    }
}
