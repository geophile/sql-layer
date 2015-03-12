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

import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.sql.optimizer.rule.cost.CostModelFactory;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.externaldata.ExternalDataService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.types.service.TypesRegistryService;

public final class ServerServiceRequirements {

    public ServerServiceRequirements(LayerInfoInterface layerInfo,
                                     DXLService dxlService,
                                     MonitorService monitor,
                                     SessionService sessionService,
                                     Store store,
                                     ConfigurationService config,
                                     IndexStatisticsService indexStatistics,
                                     TypesRegistryService typesRegistryService,
                                     RoutineLoader routineLoader,
                                     TransactionService txnService,
                                     SecurityService securityService,
                                     CostModelFactory costModel,
                                     MetricsService metricsService,
                                     ServiceManager serviceManager) {
        this.layerInfo = layerInfo;
        this.dxlService = dxlService;
        this.monitor = monitor;
        this.sessionService = sessionService;
        this.store = store;
        this.config = config;
        this.indexStatistics = indexStatistics;
        this.typesRegistryService = typesRegistryService;
        this.routineLoader = routineLoader;
        this.txnService = txnService;
        this.securityService = securityService;
        this.costModel = costModel;
        this.metricsService = metricsService;
        this.serviceManager = serviceManager;
    }

    public LayerInfoInterface layerInfo() {
        return layerInfo;
    }

    public DXLService dxl() {
        return dxlService;
    }

    public MonitorService monitor() {
        return monitor;
    }

    public SessionService sessionService() {
        return sessionService;
    }

    public Store store() {
        return store;
    }

    public TypesRegistryService typesRegistryService() {
        return typesRegistryService;
    }

    public ConfigurationService config() {
        return config;
    }

    public IndexStatisticsService indexStatistics() {
        return indexStatistics;
    }

    public RoutineLoader routineLoader() {
        return routineLoader;
    }

    public TransactionService txnService() {
        return txnService;
    }

    public SecurityService securityService() {
        return securityService;
    }

    public CostModelFactory costModel() {
        return costModel;
    }

    public MetricsService metricsService() {
        return metricsService;
    }

    public ServiceManager serviceManager() {
        return serviceManager;
    }

    /* Less commonly used, started on demand */

    public ExternalDataService externalData() {
        return serviceManager.getServiceByClass(ExternalDataService.class);
    }

    private final LayerInfoInterface layerInfo;
    private final DXLService dxlService;
    private final MonitorService monitor;
    private final SessionService sessionService;
    private final Store store;
    private final ConfigurationService config;
    private final IndexStatisticsService indexStatistics;
    private final TypesRegistryService typesRegistryService;
    private final RoutineLoader routineLoader;
    private final TransactionService txnService;
    private final SecurityService securityService;
    private final CostModelFactory costModel;
    private final MetricsService metricsService;
    private final ServiceManager serviceManager;
}
