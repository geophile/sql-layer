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

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EmbeddedJDBCITBase extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        // JDBC service is not in test-services.
        return super.serviceBindingsProvider()
            .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    public static final String SCHEMA_NAME = "test";
    public static final String CONNECTION_URL = "jdbc:default:connection";

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
    }

    protected List<List<?>> sql(String sql) {
        try (Connection conn = getConnection();
             Statement statement = conn.createStatement()) {
            if (!statement.execute(sql))
                return null;
            List<List<?>> results = new ArrayList<>();
            try (ResultSet rs = statement.getResultSet()) {
                int ncols = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>(ncols);
                    for (int i = 0; i < ncols; ++i)
                        row.add(rs.getObject(i+1));
                    results.add(row);
                }
            }
            if (statement.getMoreResults())
                throw new RuntimeException("multiple ResultSets for SQL: " + sql);
            return results;
        }
        catch (Exception ex) {
            throw new RuntimeException("while executing SQL: " + sql, ex);
        }
    }

}
