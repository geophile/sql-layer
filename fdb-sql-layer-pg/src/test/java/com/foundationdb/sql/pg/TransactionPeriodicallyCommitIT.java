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

import com.foundationdb.server.store.FDBStore;
import com.foundationdb.sql.jdbc.core.BaseConnection;
import com.foundationdb.sql.jdbc.core.ProtocolConnection;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionPeriodicallyCommitIT extends PostgresServerITBase {

    /** Delicate: STORAGE_FORMAT rowdata is ~139 per row, tuple is ~48 */
    private static final int AFTER_BYTES = 200;
    /** The number of commits it took to hit the number of bytes at which it should commit **/
    private static final int NUMBER_OF_INSERTS = 4;
    /** the number of rows per insert **/
    private static final int NUMBER_OF_ROWS = 3;
    /** a 100 byte lorem ipsum **/
    private static final String SAMPLE_STRING =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris auctor enim dui, eget egestas metus.";

    @Before
    public void createSimpleSchema() throws Exception {
        // SQL based but actually depends on row sizes and config names.
        Assume.assumeTrue("FDBStore", store() instanceof FDBStore);
        String sqlCreate = "CREATE TABLE fake.T1 (c1 integer not null primary key)";
        getConnection().createStatement().execute(sqlCreate);
    }

    @After
    public void dontLeaveConnection() throws Exception {
        // Tests change transaction periodically commit. Easiest not to reuse.
        forgetConnection();
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("fdbsql.fdb.periodically_commit.after_bytes", Integer.toString(AFTER_BYTES));
        return config;
    }

    @Test
    public void testOn() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().setAutoCommit(false);
        int lastCount = -1;
        int rowIndex = 0;
        for (int i=0; i < 10; ++i) {
            rowIndex = insertRows(rowIndex, i);
            getConnection().rollback();
            int count = getCount();
            if (i < 2) {
                assertEquals("Should not have committed anything after " + i + " statements", 0, count);
            } else {
                if (lastCount < 0) {
                    if (count > 0) {
                        lastCount = count;
                    }
                }
                else if (count > lastCount) {
                    assertEquals("Should be committing the same amount each time", lastCount*2, count);
                    return; // success, it committed twice during the transaction
                }
            }
        }
        if (lastCount < 0) {
            fail("never committed");
        } else {
            fail("only committed once");
        }
    }

    @Test
    public void testDefaultOff() throws Exception {
        testOffHelper();
    }

    @Test
    public void testExplicitlyOff() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'off'");
        testOffHelper();
    }

    @Test
    public void testUserLevel() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'userLevel'");
        getConnection().setAutoCommit(false);
        int lastCount = -1;
        int rowIndex = 0;
        for (int i=1; i < 10; i++) {
            int transactionState = -1;
            for (int j = 0; j < i; j++) {
                rowIndex = insertRow(rowIndex);
                transactionState = ((BaseConnection) getConnection()).getTransactionState();
                if (transactionState == ProtocolConnection.TRANSACTION_IDLE) {
                    rowIndex = insertRow(rowIndex);
                    break;
                }
            }
            assertNotEquals(-1,transactionState);
            getConnection().rollback();
            int count = getCount();
            if (i < 2) {
                assertEquals("Should not have committed anything after " + i + " statements", 0, count);
            } else {
                if (lastCount < 0) {
                    if (count > 0) {
                        lastCount = count;
                        assertEquals("IDLE=0, OPEN=1, FAILED=2",
                                ProtocolConnection.TRANSACTION_IDLE, transactionState);
                    } else {
                        assertEquals("IDLE=0, OPEN=1, FAILED=2",
                                ProtocolConnection.TRANSACTION_OPEN, transactionState);
                    }
                }
                else if (count > lastCount) {
                    assertEquals("Should be committing the same amount each time", lastCount*2, count);
                    assertEquals(count + " rows inserted after " + lastCount + " rows, but state not idle (OPEN is 1)",
                            ProtocolConnection.TRANSACTION_IDLE, transactionState);
                    return; // success, it committed twice during the transaction
                } else {
                    assertEquals("IDLE=0, OPEN=1, FAILED=2", ProtocolConnection.TRANSACTION_OPEN, transactionState);
                }
            }
        }
        if (lastCount < 0) {
            fail("never committed");
        } else {
            fail("only committed once");
        }
    }

    @Test
    public void testFailPartWayThroughInsertStatementOn() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(NULL, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) {}
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailPartWayThroughInsertStatementUserLevel() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'userLevel'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(NULL, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) {}
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailWithDeferredConstraintCheckOn() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().createStatement().execute("SET constraintCheckTime TO 'DEFERRED_WITH_RANGE_CACHE'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(0, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) { }
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailWithConstraintCheckOn() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(0, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) { }
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailWithConstraintCheckUserLevel() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'userLevel'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(0, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) { }
        // Note: if the above code works correctly, we'll be in an idle state, which jdbc uses to turn this into noop
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailWithDeferredConstraintCheckUserLevel() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'userLevel'");
        getConnection().createStatement().execute("SET constraintCheckTime TO 'DEFERRED_WITH_RANGE_CACHE'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(0, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) { }
        // Note: if the above code works correctly, we'll be in an idle state, which jdbc uses to turn this into noop
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    public int insertRows(int rowIndex, int i) throws Exception {
        for (int j = 0; j < i; j++) {
            rowIndex = insertRow(rowIndex);
        }
        return rowIndex;
    }

    public int insertRow(int rowIndex) throws Exception {
        getConnection().createStatement().execute(
                "insert into fake.T1 VALUES (" + rowIndex++ + "),(" + rowIndex++ + "),(" + rowIndex++ + ")");
        return rowIndex;
    }

    public void testOffHelper() throws Exception {
        getConnection().setAutoCommit(false);
        int rowIndex = 0;
        int commitAt = NUMBER_OF_INSERTS * 2;
        for (int i=0; i <= commitAt; i++) {
            rowIndex = insertRows(rowIndex, i);
            if (i < commitAt) {
                getConnection().rollback();
                assertEquals("Should not have committed anything before committing", 0, getCount());
            } else {
                getConnection().commit();
                assertEquals("Should commit eventually", NUMBER_OF_ROWS * commitAt, getCount());
            }
        }
    }

    public int getCount() throws Exception {
        ResultSet resultSet = getConnection().createStatement().executeQuery("SELECT COUNT(*) FROM fake.T1");
        assertTrue(resultSet.next());
        return resultSet.getInt(1);
    }
}
