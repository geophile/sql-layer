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
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.QueryContextBase;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.StoreAdapterHolder;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.common.types.TypesTranslator;

import java.io.IOException;

public class ServerQueryContext<T extends ServerSession> extends QueryContextBase
{
    private final T server;
    private StoreAdapterHolder storeHolder;

    public ServerQueryContext(T server) {
        this.server = server;
        this.storeHolder = server.getStoreHolder();
    }

    public T getServer() {
        return server;
    }

    @Override
    public StoreAdapter getStore() {
        assert (storeHolder != null) : "init() not called";
        return storeHolder.getAdapter();
    }

    @Override
    public StoreAdapter getStore(Table table) {
        assert (storeHolder != null) : "init() not called";
        return storeHolder.getAdapter(table);
    }

    @Override
    public Session getSession() {
        return server.getSession();
    }

    @Override 
    public AkibanInformationSchema getAIS() {
        return storeHolder.getAdapter().getAIS();
    }
    
    @Override
    public ServiceManager getServiceManager() {
        return server.getServiceManager();
    }

    @Override
    public String getCurrentUser() {
        return getSessionUser();
    }

    @Override
    public String getSessionUser() {
        return server.getProperty("user");
    }

    @Override
    public String getCurrentSchema() {
        return server.getDefaultSchemaName();
    }

    @Override
    public String getCurrentSetting(String key) {
        return server.getSessionSetting(key);
    }

    @Override
    public int getSessionId() {
        return server.getSessionMonitor().getSessionId();
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        try {
            server.notifyClient(level, errorCode, message);
        }
        catch (IOException ex) {
        }
    }

    @Override
    public long getQueryTimeoutMilli() {
        return server.getQueryTimeoutMilli();
    }

    @Override
    public ServerTransaction.PeriodicallyCommit getTransactionPeriodicallyCommit() {
        return server.getTransactionPeriodicallyCommit();
    }

    public TypesTranslator getTypesTranslator() {
        return server.typesTranslator();
    }
}
