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
package com.foundationdb.sql.server;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapterHolder;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.externaldata.ExternalDataService;
import com.foundationdb.server.service.monitor.SessionMonitor;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.sql.optimizer.AISBinderContext;
import com.foundationdb.sql.optimizer.rule.PipelineConfiguration;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;
import com.foundationdb.sql.parser.IsolationLevel;
import com.foundationdb.sql.parser.SQLParser;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/** A session has the state needed to execute SQL statements and
 * return results to the client. */
public interface ServerSession
{
    /** Return properties specified by the client. */
    public Properties getProperties();

    /** Get a client property. */
    public String getProperty(String key);

    /** Get a client property. */
    public String getProperty(String key, String defval);

    /** Get a boolean client property with error checking. */
    public boolean getBooleanProperty(String key, boolean defval);

    /** Set a client property. */
    public void setProperty(String key, String value);

    /** Get session attributes used to store state between statements. */
    public Map<String,Object> getAttributes();

    /** Get a session attribute. */
    public Object getAttribute(String key);

    /** Set a session attribute. */
    public void setAttribute(String key, Object attr);

    /** Return DXL service. */
    public DXLService getDXL();

    /** Return SQL Layer session. */
    public Session getSession();

    /** Return the default schema for SQL objects. */
    public String getDefaultSchemaName();

    /** Set the default schema for SQL objects. */
    public void setDefaultSchemaName(String defaultSchemaName);

    /** Set the currenet value of a session setting. */
    public String getSessionSetting(String key);

    /** Return server's AIS. */
    public AkibanInformationSchema getAIS();
    
    /** Return a parser for SQL statements. */
    public SQLParser getParser();
    
    /** Return the binder context. */
    public AISBinderContext getBinderContext();
    
    /** Return configured properties. */
    public Properties getCompilerProperties();

    /** Return the object used to monitor sessions. */
    public SessionMonitor getSessionMonitor();

    /** Get adapters for the given schema. */
    public StoreAdapterHolder getStoreHolder();

    /** Return the transaction service */
    public TransactionService getTransactionService();

    /** Is a transaction open? */
    public boolean isTransactionActive();

    /** Is a transaction marked rollback-only? */
    public boolean isTransactionRollbackPending();

    /** Begin a new transaction. */
    public void beginTransaction();

    /** Commit the current transaction. */
    public void commitTransaction();

    /** Rollback the current transaction. */
    public void rollbackTransaction();

    /** Set current transaction to read-only / read-write. */
    public void setTransactionReadOnly(boolean readOnly);

    /** Set following transaction to read-only / read-write. */
    public void setTransactionDefaultReadOnly(boolean readOnly);

    /** Get the current (or default) transaction isolation level. */
    public IsolationLevel getTransactionIsolationLevel();

    /** Set current transaction isolation level. */
    public IsolationLevel setTransactionIsolationLevel(IsolationLevel level);

    /** Set following transaction isolation level. */
    public IsolationLevel setTransactionDefaultIsolationLevel(IsolationLevel level);

    /** Return whether to commit as determined by store. */
    public ServerTransaction.PeriodicallyCommit getTransactionPeriodicallyCommit();

    /** Set following transaction to commit as determined by store.
     * @param periodicallyCommit*/
    public void setTransactionPeriodicallyCommit(ServerTransaction.PeriodicallyCommit periodicallyCommit);

    /** Get the server's idea of the current time. */
    public Date currentTime();

    /** Get query timeout in milliseconds or <code>null</code> if it has not been set. */
    public long getQueryTimeoutMilli();

    /** Get compatibility mode for MySQL zero dates. */
    public ServerValueEncoder.ZeroDateTimeBehavior getZeroDateTimeBehavior();

    /** Get flexible output format style */
    public FormatOptions getFormatOptions();
    
    /** Send a message to the client. */
    public void notifyClient(QueryContext.NotificationLevel level, ErrorCode errorCode, String message) throws IOException;

    /** Send a {@link QueryContext.NotificationLevel#WARNING} from the exception. */
    public void warnClient(InvalidOperationException e);

    /** Get the index cost estimator. */
    public CostEstimator costEstimator(ServerOperatorCompiler compiler, KeyCreator keyCreator);

    /** Get the overload and casts resolver */
    public TypesRegistryService typesRegistryService();

    /** Get the SQL types translator */
    public TypesTranslator typesTranslator();

    /** Get the stored procedure cache */
    public RoutineLoader getRoutineLoader();

    /** Get the external data loader / dumper */
    public ExternalDataService getExternalDataService();

    /** Get the security service */
    public SecurityService getSecurityService();

    /** Get the service manager */
    public ServiceManager getServiceManager();

    /** Check access to given schema */
    public boolean isSchemaAccessible(String schemaName);

    /** Get the pipeline configuration. */
    public PipelineConfiguration getPipelineConfiguration();

    /** Defer some foreign key constraints. */
    public void setDeferredForeignKey(ForeignKey foreignKey, boolean deferred);
}
