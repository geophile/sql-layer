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
package com.foundationdb.server.service.dxl;

import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.error.ServiceNotStartedException;
import com.foundationdb.server.error.ServiceAlreadyStartedException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class DXLServiceImpl implements DXLService, Service {
    private final static Logger LOG = LoggerFactory.getLogger(DXLServiceImpl.class);

    private final Object MONITOR = new Object();

    private volatile DDLFunctions ddlFunctions;
    private volatile DMLFunctions dmlFunctions;

    private final SchemaManager schemaManager;
    private final Store store;
    private final SessionService sessionService;
    private final IndexStatisticsService indexStatisticsService;
    private final TransactionService txnService;
    private final ListenerService listenerService;
    private final ConfigurationService configService;

    @Override
    public void start() {
        List<DXLFunctionsHook> hooks = getHooks();
        DDLFunctions localDdlFunctions = new HookableDDLFunctions(createDDLFunctions(), hooks, sessionService);
        DMLFunctions localDmlFunctions = new HookableDMLFunctions(createDMLFunctions(), hooks);
        synchronized (MONITOR) {
            if (ddlFunctions != null) {
                throw new ServiceAlreadyStartedException("DXLService");
            }
            ddlFunctions = localDdlFunctions;
            dmlFunctions = localDmlFunctions;
        }
    }

    DMLFunctions createDMLFunctions() {
        return new BasicDMLFunctions(schemaManager, store, listenerService);
    }

    DDLFunctions createDDLFunctions() {
        return new BasicDDLFunctions(schemaManager, store, indexStatisticsService,
                                     txnService, listenerService, configService);
    }

    @Override
    public void stop() {
        synchronized (MONITOR) {
            if (ddlFunctions == null) {
                throw new ServiceNotStartedException("DDL Functions stop");
            }
            ddlFunctions = null;
            dmlFunctions = null;
        }
    }

    @Override
    public DDLFunctions ddlFunctions() {
        final DDLFunctions ret = ddlFunctions;
        if (ret == null) {
            throw new ServiceNotStartedException("DDL Functions");
        }
        return ret;
    }

    @Override
    public DMLFunctions dmlFunctions() {
        final DMLFunctions ret = dmlFunctions;
        if (ret == null) {
            throw new ServiceNotStartedException("DML Functions");
        }
        return ret;
    }

    protected List<DXLFunctionsHook> getHooks() {
        return Arrays.<DXLFunctionsHook>asList(new DXLTransactionHook(txnService));
    }

    @Override
    public void crash() {
    }

    @Inject
    public DXLServiceImpl(SchemaManager schemaManager,
                          Store store,
                          SessionService sessionService,
                          IndexStatisticsService indexStatisticsService,
                          TransactionService txnService,
                          ListenerService listenerService,
                          ConfigurationService configService) {
        this.schemaManager = schemaManager;
        this.store = store;
        this.sessionService = sessionService;
        this.indexStatisticsService = indexStatisticsService;
        this.txnService = txnService;
        this.listenerService = listenerService;
        this.configService = configService;
    }
}
