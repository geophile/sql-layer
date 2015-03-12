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
package com.foundationdb.sql.embedded;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.routines.ScriptEngineManagerProvider;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.User;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.sql.optimizer.rule.cost.CostModelFactory;
import com.foundationdb.sql.server.ServerServiceRequirements;
import com.foundationdb.sql.jdbc.ProxyDriverImpl;

import java.lang.reflect.*;
import java.security.Principal;
import java.sql.*;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class EmbeddedJDBCServiceImpl implements EmbeddedJDBCService, Service {
    public static final String COMMON_PROPERTIES_PREFIX = "fdbsql.sql.";
    public static final String EMBEDDED_PROPERTIES_PREFIX = "fdbsql.embedded_jdbc.";
    private final ServerServiceRequirements reqs;
    private ScriptEngineManagerProvider scriptEngineManagerProvider;
    private JDBCDriver driver;
    private Driver proxyDriver;

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedJDBCService.class);

    @Inject
    public EmbeddedJDBCServiceImpl(LayerInfoInterface layerInfo,
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
                                   ScriptEngineManagerProvider scriptEngineManagerProvider) {
        reqs = new ServerServiceRequirements(layerInfo, dxlService, monitor,
                sessionService, store,
                config, indexStatisticsService, overloadResolutionService, 
                routineLoader, txnService, securityService, costModel,
                metricsService,
                serviceManager);
        this.scriptEngineManagerProvider = scriptEngineManagerProvider;
    }

    @Override
    public Driver getDriver() {
        return driver;
    }

    @Override
    public Connection newConnection(Properties properties, Principal user, boolean isAdminRole) throws SQLException {
        if (user != null) {
            properties.put("user", user.getName());
            properties.put("database", user.getName());
        }
        else if (!properties.containsKey("user")) {
            properties.put("user", ""); // Avoid NPE.
        }
        Connection conn = driver.connect(JDBCDriver.URL, properties);
        if (user != null) {
            reqs.securityService().setAuthenticated(((JDBCConnection)conn).getSession(),
                                                    user, isAdminRole);
        }
        return conn;
    }

    @Override
    public void start() {
        Properties properties = reqs.config().deriveProperties(COMMON_PROPERTIES_PREFIX);
        properties.putAll(reqs.config().deriveProperties(EMBEDDED_PROPERTIES_PREFIX));
        driver = new JDBCDriver(reqs, properties);
        try {
            Class<?> proxyDriverClazz = Class.forName(ProxyDriverImpl.class.getName(), true, this.scriptEngineManagerProvider.getSafeClassLoader());
            Constructor<?> proxyConstructor = proxyDriverClazz.getConstructor(Driver.class);
            Object proxyDriverInstanceObject = proxyConstructor.newInstance(driver);
            proxyDriver = (Driver) proxyDriverInstanceObject;
            driver.register();
            registerProxy(proxyDriver);
        }
        catch (ClassNotFoundException cnfe) {
            throw new AkibanInternalException("Cannot find proxy driver class", cnfe);
        }
        catch (NoSuchMethodException nsme) {
            throw new AkibanInternalException("Cannot find constructor method driver", nsme);
        }
        catch (IllegalAccessException iae) {
            throw new AkibanInternalException("Cannot access proxy driver constructor", iae);
        }
        catch (InstantiationException ie) {
            throw new AkibanInternalException("Cannot instantiate proxy driver", ie);
        }
        catch (InvocationTargetException ite){
            throw new AkibanInternalException("Cannot instantiate proxy driver", ite);
        }
        catch (SQLException ex) {
            throw new AkibanInternalException("Cannot register driver with JDBC", ex);
        }
    }

    @Override
    public void stop() {
        if (driver != null) {
            try {
                deregisterProxy(proxyDriver);
                driver.deregister();
            }
            catch (SQLException ex) {
                logger.warn("Cannot deregister embedded driver with JDBC", ex);
            }
            driver = null;
            proxyDriver = null;
        }
    }

    @Override
    public void crash() {
        stop();
    }

    private void registerProxy(Driver driver) throws SQLException {
        DriverManager.registerDriver(driver);
    }

    private void deregisterProxy(Driver driver) throws SQLException {
        try {
            Class<?> deregisterProxyDriverHelper = Class.forName("com.foundationdb.sql.jdbc.DeregisterProxyDriverHelper", true, proxyDriver.getClass().getClassLoader());
            Object dph = deregisterProxyDriverHelper.newInstance();
            Method method = deregisterProxyDriverHelper.getMethod("deregisterProxy", Driver.class);
            method.invoke(dph, proxyDriver);
        }
        catch (NoSuchMethodException nme) {
            throw new AkibanInternalException("deregisterPoxy method does not exist", nme);
        }
        catch (InvocationTargetException ite) {
            throw new AkibanInternalException("Cannot deregister proxy driver", ite);
        }
        catch (ClassNotFoundException cnfe) {
            throw new AkibanInternalException("Cannot find DeregisterProxyDriverHelper class", cnfe);
        }
        catch (IllegalAccessException iae) {
            throw new AkibanInternalException("Cannot access DeregisterProxyDriverHelper", iae);
        }
        catch (InstantiationException ie) {
            throw new AkibanInternalException("Cannot instantiate DeregisterProxyDriverHelper", ie);
        }
    }
}
