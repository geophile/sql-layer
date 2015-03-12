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
package com.foundationdb.server.test.it.isolation;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.sql.embedded.JDBCConnection;

import java.sql.*;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadCommittedIsolationDT extends IsolationITBase
{
    private static final int NROWS = 10000;

    @Before
    public void populate() throws SQLException {
        createTable(SCHEMA_NAME, "t1", "id INT PRIMARY KEY, n INT");
        Properties props = new Properties();
        props.put("user", SCHEMA_NAME);
        props.put("constraintCheckTime", "DELAYED_WITH_RANGE_CACHE_ALWAYS_UNTIL_COMMIT");
        try (Connection conn = DriverManager.getConnection(CONNECTION_URL, props);
             PreparedStatement stmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {
            conn.setAutoCommit(false);
            for (int i = 0; i < NROWS; i++) {
                stmt.setInt(1, i);
                stmt.setInt(2, i);
                stmt.executeUpdate();
            }
            conn.commit();
        }
    }

    protected void slowScan() throws SQLException {
        int sum = 0;
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t1")) {
            while (rs.next()) {
                sum += rs.getInt(2);
                try {
                    Thread.sleep(6);
                }
                catch (InterruptedException ex) {
                    break;
                }
            }
        }
        assertEquals((NROWS * (NROWS - 1)) / 2, sum);
    }

    protected void manyScans() throws SQLException {
        int sum = 0;
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT n FROM t1 WHERE id = ?")) {
            for (int i = 0; i < NROWS; i++) {
                stmt.setInt(1, i);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        sum += rs.getInt(1);
                    }
                }
                try {
                    Thread.sleep(6);
                }
                catch (InterruptedException ex) {
                    break;
                }
            }
        }
        assertEquals((NROWS * (NROWS - 1)) / 2, sum);
    }

    @Test
    @Isolation(Connection.TRANSACTION_SERIALIZABLE)
    @SQLExceptionExpected(errorCode = ErrorCode.FDB_PAST_VERSION)
    public void slowScanPastVersion() throws SQLException {
        slowScan();
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT)
    public void slowScanReadCommitted() throws SQLException {
        slowScan();
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_SERIALIZABLE_SNAPSHOT)
    @SQLExceptionExpected(errorCode = ErrorCode.FDB_PAST_VERSION)
    public void slowScanSnapshotAlsoPastVersion() throws SQLException {
        slowScan();
    }

    @Test
    @Isolation(Connection.TRANSACTION_SERIALIZABLE)
    @SQLExceptionExpected(errorCode = ErrorCode.FDB_PAST_VERSION)
    public void manyScansPastVersion() throws SQLException {
        manyScans();
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT)
    public void manyScansReadCommitted() throws SQLException {
        manyScans();
    }

}
