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

import com.foundationdb.KeyValue;
import com.foundationdb.KeySelector;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.async.Future;
import com.foundationdb.async.ReadyFuture;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;

/**
 * An iterator over <code>KeyValue</code> pairs that commits its
 * transaction as necessary.
 */
public class FDBScanCommittingIterator implements AsyncIterator<KeyValue>
{
    private static final Logger logger = LoggerFactory.getLogger(FDBScanCommittingIterator.class);

    private final TransactionState transaction;
    private final KeySelector start, end;
    private final int limit;
    private final boolean reverse;
    private final FDBScanTransactionOptions options;
    private AsyncIterator<KeyValue> underlying = null;
    private KeyValue lastKeyValue = null;
    private int count, totalCount = 0;
    private int resetCount;
    
    public FDBScanCommittingIterator(TransactionState transaction,
                                     KeySelector start, KeySelector end,
                                     int limit, boolean reverse,
                                     FDBScanTransactionOptions options) {
        this.transaction = transaction;
        this.start = start;
        this.end = end;
        this.limit = limit;
        this.reverse = reverse;
        this.options = options;
    }

    @Override
    public boolean hasNext() {
        checkForRestart();
        if (underlying == null) return false;
        return underlying.hasNext();
    }

    @Override
    public KeyValue next() {
        checkForRestart();
        if (underlying == null) throw new NoSuchElementException();
        lastKeyValue = underlying.next();
        count++;
        return lastKeyValue;
    }
    
    @Override
    public Future<Boolean> onHasNext() {
        checkForRestart();
        if (underlying == null) return new ReadyFuture<>(Boolean.FALSE);
        return underlying.onHasNext();
    }

    @Override
    public void cancel() {
        if (underlying != null) {
            underlying.cancel();
        }
    }

    @Override
    public void dispose() {
        if (underlying != null) {
            underlying.dispose();
            underlying = null;
        }
    }

    protected void checkForRestart() {
        boolean dispose = false, commit = false;
        if (underlying != null) {
            if (resetCount != transaction.getResetCount()) {
                logger.debug("Updating for current transaction");
                dispose = true;
            }
            else if (options.shouldCommitAfterRows(count) ||
                     options.shouldCommitAfterMillis(transaction.getStartTime())) {
                logger.debug("Commit after {} rows", count);
                dispose = commit = true;
            }
        }
        else {
            if (options.shouldCommitAfterMillis(transaction.getStartTime())) {
                logger.debug("Commit before starting");
                commit = true;
            }
        }
        if (dispose) {
            totalCount += count;
            dispose();
        }
        if (commit) {
            transaction.commitAndReset();
            try {
                options.maybeSleepAfterCommit();
            }
            catch (InterruptedException ex) {
                throw new QueryCanceledException(transaction.getSession());
            }
        }
        if (underlying == null) {
            KeySelector start = this.start, end = this.end;
            int limit = this.limit;
            if (lastKeyValue != null) {
                if (reverse) {
                    end = KeySelector.lastLessThan(lastKeyValue.getKey());
                }
                else {
                    start = KeySelector.firstGreaterThan(lastKeyValue.getKey());
                }
            }
            if (limit != Transaction.ROW_LIMIT_UNLIMITED) {
                limit -= totalCount;
            }
            if (options.isSnapshot()) {
                underlying = transaction.getSnapshotRangeIterator(start, end, limit, reverse);
            }
            else {
                underlying = transaction.getRangeIterator(start, end, limit, reverse);
            }
            count = 0;
            resetCount = transaction.getResetCount();
        }
    }

    @Override
    public void remove() {
        if (underlying != null) {
            underlying.remove();
        }
        else {
            throw new IllegalStateException();
        }
    }

}
