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
package com.foundationdb.sql.jdbc;

import java.sql.Connection;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class ProxyDriverImpl implements java.sql.Driver {

    Driver embeddedJDBCDriver;
    
    public ProxyDriverImpl(Driver embeddedJDBCDriver) {
        this.embeddedJDBCDriver = embeddedJDBCDriver;    
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return this.embeddedJDBCDriver.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return this.embeddedJDBCDriver.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return this.embeddedJDBCDriver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return this.embeddedJDBCDriver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return this.embeddedJDBCDriver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return this.embeddedJDBCDriver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return this.embeddedJDBCDriver.getParentLogger();
    }
}
