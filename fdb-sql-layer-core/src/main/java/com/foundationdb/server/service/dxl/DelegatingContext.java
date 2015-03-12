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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.QueryContextBase;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;

public class DelegatingContext extends QueryContextBase
{
    private final StoreAdapter adapter;
    private final QueryContext delegate;

    public DelegatingContext(StoreAdapter adapter, QueryContext delegate) {
        this.adapter = adapter;
        this.delegate = (delegate == null) ? new SimpleQueryContext(adapter) : delegate;
    }

    @Override
    public StoreAdapter getStore() {
        return adapter;
    }

    @Override
    public StoreAdapter getStore(Table table) {
        return adapter;
    }

    @Override
    public Session getSession() {
        return delegate.getSession();
    }

    @Override 
    public AkibanInformationSchema getAIS() {
        return delegate.getAIS();
    }
    
    @Override
    public ServiceManager getServiceManager() {
        return delegate.getServiceManager();
    }

    @Override
    public String getCurrentUser() {
        return delegate.getCurrentUser();
    }

    @Override
    public String getSessionUser() {
        return delegate.getSessionUser();
    }

    @Override
    public String getCurrentSchema() {
        return delegate.getCurrentSchema();
    }

    @Override
    public String getCurrentSetting(String key) {
        return delegate.getCurrentSetting(key);
    }

    @Override
    public int getSessionId() {
        return delegate.getSessionId();
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        delegate.notifyClient(level, errorCode, message);
    }

    @Override
    public long getQueryTimeoutMilli() {
        return delegate.getQueryTimeoutMilli();
    }
}
