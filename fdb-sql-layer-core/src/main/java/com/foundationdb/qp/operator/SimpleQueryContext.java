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
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link QueryContext} for use without a full server for internal plans / testing. */
public class SimpleQueryContext extends QueryContextBase
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleQueryContext.class);

    private final StoreAdapter adapter;
    private final ServiceManager serviceManager;

    public SimpleQueryContext() {
        this(null);
    }

    public SimpleQueryContext(StoreAdapter adapter) {
        this(adapter, null);
    }

    public SimpleQueryContext(StoreAdapter adapter, ServiceManager serviceManager) {
        this.adapter = adapter;
        this.serviceManager = serviceManager;
    }

    @Override
    public StoreAdapter getStore() {
        requireAdapter();
        return adapter;
    }

    @Override
    public StoreAdapter getStore(Table table) {
        requireAdapter();
        return adapter;
    }
    
    @Override
    public Session getSession() {
    	if (adapter != null)
            return adapter.getSession();
    	else
            return null;
    }

    @Override
    public ServiceManager getServiceManager() {
        requireServiceManager();
        return serviceManager;
    }

    @Override
    public String getCurrentUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSessionUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentSchema() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentSetting(String key) {
        return null;
    }

    @Override
    public AkibanInformationSchema getAIS() {
        return adapter.getAIS();
    }
    
    @Override
    public int getSessionId() {
        requireAdapter();
        return (int)adapter.getSession().sessionId();
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        switch (level) {
        case WARNING:
            logger.warn("{} {}", errorCode, message);
            break;
        case INFO:
            logger.info("{} {}", errorCode, message);
            break;
        case DEBUG:
            logger.debug("{} {}", errorCode, message);
            break;
        }
    }

    @Override
    public void checkQueryCancelation() {
        if (adapter.getSession() != null) {
           super.checkQueryCancelation();
        }
    }

    private void requireAdapter() {
        if(adapter == null) {
            throw new UnsupportedOperationException("Not constructed with StoreAdapter");
        }
    }

    private void requireServiceManager() {
        if(serviceManager == null) {
            throw new UnsupportedOperationException("Not constructed with ServiceManager");
        }
    }
}
