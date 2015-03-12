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

import com.foundationdb.sql.server.ServerServiceRequirements;
import com.foundationdb.server.service.monitor.ServerMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class JDBCDriver implements Driver, ServerMonitor {
    public static final String URL = "jdbc:default:connection";

    private final ServerServiceRequirements reqs;
    private final Properties properties;
    private long startTime;
    private int nconnections;

    private static final Logger logger = LoggerFactory.getLogger(JDBCDriver.class);

    protected JDBCDriver(ServerServiceRequirements reqs, Properties properties) {
        this.reqs = reqs;
        this.properties = properties;
    }

    public void register() throws SQLException {
        startTime = System.currentTimeMillis();
        DriverManager.registerDriver(this);
        reqs.monitor().registerServerMonitor(this);
    }

    public void deregister() throws SQLException {
        reqs.monitor().deregisterServerMonitor(this);
        DriverManager.deregisterDriver(this);
    }

    /* Driver */

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!url.equals(URL)) return null;
        nconnections++;
        Properties connProps = new Properties(properties);
        if (info != null) {
            for (String prop : info.stringPropertyNames()) { // putAll would not inherit.
                connProps.put(prop, info.getProperty(prop));
            }
        }
        return new JDBCConnection(reqs, connProps);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.equals(URL);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }


    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() 
            throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Uses LOG4J");
    }

    /* ServerMonitor */

    @Override
    public String getServerType() {
        return JDBCConnection.SERVER_TYPE;
    }

    @Override
    public int getLocalPort() {
        return -1;
    }

    @Override
    public String getLocalHost() {
        return null;
    }

    @Override
    public long getStartTimeMillis() {
        return startTime;
    }
    
    @Override
    public int getSessionCount() {
        return nconnections;
    }

}
