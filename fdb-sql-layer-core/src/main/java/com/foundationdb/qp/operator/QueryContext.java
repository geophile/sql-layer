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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.sql.server.ServerTransaction;

import java.util.Date;

/** The context for the execution of a query.
 * Associated the query with the running environment.
 * The context is global for the whole query; it does not change for
 * different iteration values at different places in the query.
 */
public interface QueryContext
{
    /**
     * Get the store associated with this query.
     */
    public StoreAdapter getStore();
    public StoreAdapter getStore(Table table);

    /**
     * Get the AkibanInformationSchema associated with this context
     */
    public AkibanInformationSchema getAIS();
    
    /**
     * Get the session associated with this context.
     */
    public Session getSession();

    /**
     * Get the service manager.
     */
    public ServiceManager getServiceManager();

    /**
     * Get the current date.
     * This time may be frozen from the start of a transaction.
     */
    public Date getCurrentDate();

    /**
     * Get the current user name.
     */
    public String getCurrentUser();

    /**
     * Get the server user name.
     */
    public String getSessionUser();

    /**
     * Get the system identity of the server process.
     */
    public String getSystemUser();

    /**
     * Get the current schema name.
     */
    public String getCurrentSchema();

    /**
     * Get the current value of a session setting.
     */
    public String getCurrentSetting(String key);

    /**
     * Get the server session id.
     */
    public int getSessionId();

    /**
     * Get the system time at which the query started.
     */
    public long getStartTime();

    /**
     * Possible notification levels for {@link #notifyClient}.
     */
    public enum NotificationLevel {
        WARNING, INFO, DEBUG
    }

    /**
     * Send a warning (or other) notification to the remote client.
     * The message will be delivered as part of the current operation,
     * perhaps immediately or perhaps at its completion, depending on
     * the implementation.
     */
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message);

    /**
     * Send a warning notification to the remote client from the given exception.
     */
    public void warnClient(InvalidOperationException exception);

    /** Get the query timeout in milliseconds or <code>-1</code> if no limit. */
    public long getQueryTimeoutMilli();

    /** Check whether query has been cancelled or timeout has been exceeded. */
    public void checkQueryCancelation();

    /** Does this context commit periodically? */
    public ServerTransaction.PeriodicallyCommit getTransactionPeriodicallyCommit();

    /**
     * Create a new empty set of bindings.
     */
    public QueryBindings createBindings();
}
