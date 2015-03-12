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
package com.foundationdb.server.service.transaction;

import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.sql.parser.IsolationLevel;

import java.util.concurrent.Callable;

public interface TransactionService extends Service {
    interface Callback {
        void run(Session session, long timestamp);
    }

    interface CloseableTransaction extends AutoCloseable {
        void commit();
        void rollback();
        @Override
        void close();
    }

    enum CallbackType {
        /** Invoked <i>before</i> attempting to commit. */
        PRE_COMMIT,
        /** Invoked <i>after</i> commit completes successfully. */
        COMMIT,
        /** Invoked <i>after</i> a commit fails or rollback is manually invoked. */
        ROLLBACK,
        /** Invoked <i>after</i> a transaction ends, either by commit or rollback. */
        END
    }

    /** Returns true if there is a transaction active for the given Session */
    boolean isTransactionActive(Session session);

    /** Returns true if there is a transaction active for the given Session */
    boolean isRollbackPending(Session session);

    /** Returns the start timestamp for the open transaction. */
    long getTransactionStartTimestamp(Session session);

    /** Begin a new transaction. */
    void beginTransaction(Session session);

    /** Begin a new transaction that will rollback upon close if not committed. */
    CloseableTransaction beginCloseableTransaction(Session session);

    /** Commit the open transaction. */
    void commitTransaction(Session session);

    /**
     * Commit the transaction and reset for immediate use if a retryable exception occurs.
     *
     * <p>
     *     If the commit was successful <i>or</i> a non-retryable exception occurred, the
     *     session's transaction state is cleared and {@link #beginTransaction} must be
     *     called before further use. A non-retryable exception is immediately propagated.
     *     Otherwise, the state is reset and ready for use.
     * </p>
     *
     * @return <code>true</code> if the caller should retry.
     */
    boolean commitOrRetryTransaction(Session session);

    /** Rollback an open transaction. */
    void rollbackTransaction(Session session);

    /** Rollback the current transaction if open, otherwise do nothing. */
    void rollbackTransactionIfOpen(Session session);

    /**
     * Commit the transaction if this is a good time.
     *
     * <p>
     *     If the commit was successful, the transaction state is reset and ready
     *     for reuse. Otherwise, the exception is propagated and
     *     {@link #rollbackTransaction} must be called be called before reuse.
     * </p>
     *
     * @return {@code true} if a commit was performed, false otherwise
     */
    boolean periodicallyCommit(Session session);

    /** @return {@code true} if this is a good time to commit, {@code false} otherwise */
    boolean shouldPeriodicallyCommit(Session session);

    /** Add a callback to transaction. */
    void addCallback(Session session, CallbackType type, Callback callback);

    /** Add a callback to transaction that is required to be active. */
    void addCallbackOnActive(Session session, CallbackType type, Callback callback);

    /** Add a callback to transaction that is required to be inactive. */
    void addCallbackOnInactive(Session session, CallbackType type, Callback callback);

    /** Wrap <code>runnable</code> in a <code>Callable</code> and invoke {@link #run(Session, Callable)}. */
    void run(Session session, Runnable runnable);

    /**
     * Execute in a new transaction and automatically retry if a rollback exception occurs.
     * <p>Note: A plain <code>Exception</code> from <code>callable</code> will be <i>rethrown</i>, not retried.</p>
     */
    <T> T run(Session session, Callable<T> callable);

    enum SessionOption { 
        /** Control when / how constraints like uniqueness are checked. */
        CONSTRAINT_CHECK_TIME
    }

    /** Set user option on <code>Session</code>. */
    void setSessionOption(Session session, SessionOption option, String value);

    /** If a transaction can fail with a rollback transaction after
     * actually having succeeded, store a unique counter associated
     * with the given session persistently so that it can be checked.
     */
    int markForCheck(Session session);

    /** Check for success from given counter stored in previous
     * transaction that failed due with the given exception.
     */
    boolean checkSucceeded(Session session, Exception retryException, int sessionCounter);

    /** Defer some foreign key checks within this transaction. */
    void setDeferredForeignKey(Session session, ForeignKey foreignKey, boolean deferred);

    /** Repeat any checks that are scoped to the statement. */
    void checkStatementConstraints(Session session);

    /** Check if checks should be performed immediately. */
    boolean getForceImmediateForeignKeyCheck(Session session);

    /** Set if checks should be performed immediately (true disables all deferring). Return previous value. */
    boolean setForceImmediateForeignKeyCheck(Session session, boolean force);

    /** Return the isolation level that would be in effect if the given one were requested. */
    IsolationLevel actualIsolationLevel(IsolationLevel level);

    /** Set isolation level for the session's transaction. */
    IsolationLevel setIsolationLevel(Session session, IsolationLevel level);

    /** Does this isolation level only work with read-only transactions? */
    boolean isolationLevelRequiresReadOnly(Session session, boolean commitNow);

}
