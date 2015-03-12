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
package com.foundationdb.server.service;

import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.server.error.ServiceAlreadyStartedException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.servicemanager.ServiceManagerBase;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.stats.StatisticsService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ServiceManager extends ServiceManagerBase {
    static final Logger logger = LoggerFactory.getLogger(ServiceManager.class);

    enum State { IDLE, STARTING, ACTIVE, STOPPING, ERROR_STARTING };

    State getState();

    void startServices() throws ServiceAlreadyStartedException;

    void stopServices() throws Exception;
    
    void crashServices() throws Exception;

    ConfigurationService getConfigurationService();
    
    LayerInfoInterface getLayerInfo();

    Store getStore();

    SchemaManager getSchemaManager();

    StatisticsService getStatisticsService();

    SessionService getSessionService();

    <T> T getServiceByClass(Class<T> serviceClass);

    DXLService getDXL();

    boolean serviceIsStarted(Class<?> serviceClass);
    
    MonitorService getMonitorService();

    boolean serviceIsBoundTo(Class<?> serviceClass, Class<?> implClass);
}
