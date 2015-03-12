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
package com.foundationdb.qp.operator;

import com.foundationdb.server.error.*;
import com.foundationdb.sql.server.ServerTransaction;

import java.util.Date;

public abstract class QueryContextBase implements QueryContext
{
    // startTimeMsec is used to control query timeouts.
    private final long startTimeMsec = System.currentTimeMillis();
    private long queryTimeoutMsec = Long.MAX_VALUE;

    /* QueryContext interface */

    @Override
    public Date getCurrentDate() {
        return new Date();
    }

    @Override
    public String getSystemUser() {
        return System.getProperty("user.name");
    }

    @Override
    public long getStartTime() {
        return startTimeMsec;
    }

    @Override
    public void warnClient(InvalidOperationException exception) {
        notifyClient(NotificationLevel.WARNING, exception.getCode(), exception.getShortMessage());
    }

    @Override
    public long getQueryTimeoutMilli() {
        if (queryTimeoutMsec == Long.MAX_VALUE) {
            queryTimeoutMsec = getStore().getQueryTimeoutMilli();
        }
        return queryTimeoutMsec;
    }

    @Override
    public void checkQueryCancelation() {
        if (getSession().isCurrentQueryCanceled()) {
            throw new QueryCanceledException(getSession());
        }
        long queryTimeoutMilli = getQueryTimeoutMilli();
        if (queryTimeoutMilli >= 0) {
            long runningTimeMsec = System.currentTimeMillis() - getStartTime();
            if (runningTimeMsec > queryTimeoutMilli) {
                throw new QueryTimedOutException(runningTimeMsec);
            }
        }
    }

    @Override
    public ServerTransaction.PeriodicallyCommit getTransactionPeriodicallyCommit() {
        return ServerTransaction.PeriodicallyCommit.OFF;
    }

    @Override
    public QueryBindings createBindings() {
        return new SparseArrayQueryBindings();
    }
}
