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
package com.foundationdb.sql.pg;

import com.foundationdb.server.error.ServiceAlreadyStartedException;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.is.BasicInfoSchemaTablesService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.sql.optimizer.rule.cost.CostModelFactory;
import com.foundationdb.sql.server.ServerServiceRequirements;

import com.google.inject.Inject;

/** The PostgreSQL server service.
 * @see PostgresServer
*/
public class PostgresServerManager implements PostgresService, Service {
    private final ServerServiceRequirements reqs;
    private final BasicInfoSchemaTablesService infoSchemaService;
    private PostgresServer server = null;

    @Inject
    public PostgresServerManager(LayerInfoInterface layerInfo,
                                 DXLService dxlService,
                                 MonitorService monitor,
                                 SessionService sessionService,
                                 Store store,
                                 ConfigurationService config,
                                 IndexStatisticsService indexStatisticsService,
                                 TypesRegistryService overloadResolutionService,
                                 RoutineLoader routineLoader,
                                 TransactionService txnService,
                                 SecurityService securityService,
                                 CostModelFactory costModel,
                                 MetricsService metricsService,
                                 ServiceManager serviceManager,
                                 BasicInfoSchemaTablesService infoSchemaService) {
        reqs = new ServerServiceRequirements(layerInfo, dxlService, monitor,
                sessionService, store,
                config, indexStatisticsService, overloadResolutionService, 
                routineLoader, txnService, securityService, costModel,
                metricsService,
                serviceManager);
        this.infoSchemaService = infoSchemaService;
    }

    @Override
    public void start() throws ServiceAlreadyStartedException {
        infoSchemaService.setPostgresTypeMapper(new BasicInfoSchemaTablesService.PostgresTypeMapper() {
                @Override
                public long getOid(TInstance type) {
                    return PostgresType.fromTInstance(type).getOid();
                }
            });
        server = new PostgresServer(reqs);
        server.start();
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    @Override
    public void crash() {
        stop();
    }

    /*** PostgresService ***/

    @Override
    public int getPort() {
        return server.getPort();
    }

    @Override
    public String getHost() {
        return server.getHost();
    }

    @Override
    public PostgresServer getServer() {
        return server;
    }

 }
