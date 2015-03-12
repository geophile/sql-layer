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
package com.foundationdb.sql;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.sql.optimizer.rule.cost.TestCostEstimator.TestCostModelFactory;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerOperatorCompiler;
import com.foundationdb.sql.server.ServerServiceRequirements;
import com.foundationdb.sql.server.ServerSessionBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ServerSessionITBase extends ITBase {
    public static final String SCHEMA_NAME = "test";

    protected List<String> warnings = null;

    protected List<String> getWarnings() {
        return warnings;
    }

    protected class TestQueryContext extends ServerQueryContext<TestSession> {
        public TestQueryContext(TestSession session) {
            super(session);
        }
    }

    protected class TestOperatorCompiler extends ServerOperatorCompiler {
        public TestOperatorCompiler(TestSession session) {
            initServer(session, store());
            initDone();
        }
    }

    protected class TestSession extends ServerSessionBase {
        public TestSession() {
            super(new ServerServiceRequirements(serviceManager().getLayerInfo(),
                                                dxl(),
                                                serviceManager().getMonitorService(),
                                                serviceManager().getSessionService(),
                                                store(),
                                                configService(),
                                                serviceManager().getServiceByClass(IndexStatisticsService.class),
                                                serviceManager().getServiceByClass(TypesRegistryService.class),
                                                routineLoader(),
                                                txnService(),
                                                serviceManager().getServiceByClass(SecurityService.class),
                                                new TestCostModelFactory(),
                                                serviceManager().getServiceByClass(MetricsService.class),
                                                serviceManager()));
            session = session();
            ais = ais();
            defaultSchemaName = SCHEMA_NAME;
            properties = new Properties();
            properties.put("database", defaultSchemaName);
            properties.put(CONFIG_PARSER_FEATURES, configService().getProperty("fdbsql.sql." + CONFIG_PARSER_FEATURES));
            initParser();        
            TestOperatorCompiler compiler = new TestOperatorCompiler(this);
            initAdapters(compiler);
        }

        @Override
        protected void sessionChanged() {
        }

        @Override
        public void notifyClient(QueryContext.NotificationLevel level, ErrorCode errorCode, String message) {
            if (warnings == null)
                warnings = new ArrayList<>();
            warnings.add(message);
        }
    }

}
