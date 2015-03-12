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
package com.foundationdb.server.test.qt;

import com.foundationdb.sql.pg.PostgresService;
import com.foundationdb.sql.pg.PostgresServerITBase;

import java.sql.*;

public class PostgresBulkInsertQT extends BulkInsertQT
{
    @Override
    public Connection getConnection() throws SQLException {
        int port = getPostgresService().getPort();
        if (port <= 0) {
            throw new RuntimeException("fdbsql.postgres.port is not set.");
        }
        String url = String.format(PostgresServerITBase.CONNECTION_URL,
                                   getPostgresService().getHost(),
                                   port);
        return DriverManager.getConnection(url,
                                           PostgresServerITBase.USER_NAME,
                                           PostgresServerITBase.USER_PASSWORD);
    }

    protected PostgresService getPostgresService() {
        return serviceManager().getServiceByClass(PostgresService.class);
    }
}
