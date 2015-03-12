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

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SnapshotIsolationIT extends IsolationITBase
{
    private static final int NBALLS = 10;

    @Before
    public void populate() {
        int tid = createTable(SCHEMA_NAME, "balls", "id INT PRIMARY KEY, color VARCHAR(8)");
        for (int i = 0; i < NBALLS; i++) {
            writeRow(tid, i, (i & 1) == 0 ? "black" : "white");
        }
    }

    protected void switchColors() throws SQLException {
        try (Connection conn1 = getConnection();
             Statement stmt1 = conn1.createStatement();
             Connection conn2 = getConnection();
             Statement stmt2 = conn2.createStatement()) {

            int n = stmt1.executeUpdate("UPDATE balls SET color = 'white' WHERE color = 'black'");
            assertEquals("black balls updated", NBALLS / 2, n);

            n = stmt2.executeUpdate("UPDATE balls SET color = 'black' WHERE color = 'white'");
            assertEquals("white balls updated", NBALLS / 2, n);

            conn1.commit();
            conn2.commit();
        }
    }

    @Test
    @Isolation(Connection.TRANSACTION_SERIALIZABLE)
    @SQLExceptionExpected(errorCode = ErrorCode.FDB_NOT_COMMITTED)
    public void serializableConflicts() throws SQLException {
        switchColors();
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_SERIALIZABLE_SNAPSHOT)
    public void snapshotSkews() throws SQLException {
        switchColors();
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT color, COUNT(*) FROM balls GROUP BY 1 ORDER BY 1")) {
            assertTrue(rs.next());
            assertEquals("black", rs.getString(1));
            assertEquals("black count", NBALLS / 2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("white", rs.getString(1));
            assertEquals("white count", NBALLS / 2, rs.getInt(2));
        }
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_SERIALIZABLE_SNAPSHOT)
    public void snapshotRYW() throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            assertEquals(NBALLS / 2, stmt.executeUpdate("UPDATE balls SET color = 'blue' WHERE color = 'black'"));
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM balls WHERE color <> 'black'")) {
                rs.next();
                assertEquals("See combined snapshot", NBALLS, rs.getInt(1));
            }
        }
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_SERIALIZABLE_SNAPSHOT)
    @SQLExceptionExpected(errorCode = ErrorCode.FDB_NOT_COMMITTED)
    public void snapshotVsDDL() throws SQLException {
        try(Connection conn1 = getConnection();
            Statement stmt1 = conn1.createStatement();
            Connection conn2 = getAutoCommitConnection();
            Statement stmt2 = conn2.createStatement()) {
            stmt1.executeUpdate("INSERT INTO balls VALUES (" + NBALLS + 1 + ", 'red')");
            stmt2.executeUpdate("ALTER TABLE balls ADD COLUMN size INT");
            // This fails (correctly) because AIS reads are always non-snapshot
            conn1.commit();
        }
    }
}
