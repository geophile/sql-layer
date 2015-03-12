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

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.ErrorCode;
import org.junit.Before;
import org.junit.Test;
import com.foundationdb.sql.jdbc.PGStatement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

public class PostgresServerPreparedStatementIT extends PostgresServerITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);
    private static final int ROW_COUNT = 5;

    @Before
    public void createAndInsert() {
        int tid = createTable(TABLE_NAME, "id int not null primary key, x int");
        for(long i = 1; i <= ROW_COUNT; ++i) {
            writeRow(tid, i, i*10);
        }
    }

    PreparedStatement newDropTable() throws Exception {
        return getConnection().prepareStatement("DROP TABLE "+TABLE_NAME);
    }

    PreparedStatement newCreateTable() throws Exception {
        return getConnection().prepareStatement("CREATE TABLE "+TABLE_NAME+"2(id int)");
    }

    PreparedStatement newCreateIndex() throws Exception {
        return getConnection().prepareStatement("CREATE INDEX x ON "+TABLE_NAME+"(x)");
    }

    PreparedStatement newDropIndex() throws Exception {
        return getConnection().prepareStatement("DROP INDEX x");
    }

    PreparedStatement newScan() throws Exception {
        PreparedStatement p = getConnection().prepareStatement("SELECT * FROM "+TABLE_NAME);
        // driver has lower bound of usage before fully using a named statement, this drops that
        if(p instanceof PGStatement) {
            PGStatement pgp = (PGStatement) p;
            pgp.setPrepareThreshold(1);
        }
        return p;
    }

    PreparedStatement newInsert() throws Exception {
        return getConnection().prepareStatement("INSERT INTO "+TABLE_NAME+" VALUES (100,1000)");
    }

    private static int countRows(ResultSet rs) throws SQLException {
        int count = 0;
        while(rs.next()) {
            ++count;
        }
        rs.close();
        return count;
    }

    private static List<List<Object>> listRows(ResultSet rs, int ncols) throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>(ncols);
            for (int i = 0; i < ncols; i++) {
                row.add(rs.getObject(i+1));
            }
            rows.add(row);
        }
        rs.close();
        return rows;
    }

    private static void expectStale(PreparedStatement p) {
        try {
            p.executeQuery();
            fail("Expected exception");
        } catch(SQLException e) {
            assertEquals("Error code from exception", ErrorCode.STALE_STATEMENT.getFormattedValue(), e.getSQLState());
        }
    }

    @Test
    public void fullScan() throws Exception {
        PreparedStatement p = newScan();
        ResultSet rs = p.executeQuery();
        assertEquals("Scanned row count", ROW_COUNT, countRows(rs));
        p.close();
    }

    @Test
    public void singleRowInsert() throws Exception {
        PreparedStatement p = newInsert();
        int count = p.executeUpdate();
        assertEquals("Inserted count", 1, count);
        p.close();
    }

    @Test
    public void createIndex() throws Exception {
        PreparedStatement p = newCreateIndex();
        int count = p.executeUpdate();
        assertEquals("Count from create index", 0, count);
        assertNotNull("Found new index", ais().getTable(TABLE_NAME).getIndex("x"));
    }

    @Test
    public void dropIndex() throws Exception {
        createIndex(SCHEMA, TABLE, "x", "x");
        PreparedStatement p = newDropIndex();
        int count = p.executeUpdate();
        assertEquals("Count from drop index", 0, count);
        assertNull("Index is gone", ais().getTable(TABLE_NAME).getIndex("x"));
    }

    @Test
    public void dropTableInvalidates() throws Exception {
        PreparedStatement pScan = newScan();
        PreparedStatement pDrop = newDropTable();
        assertEquals("Row count from scan1", ROW_COUNT, countRows(pScan.executeQuery()));
        int count = pDrop.executeUpdate();
        assertEquals("Count from drop table", 0, count);
        expectStale(pScan);
        pScan.close();
        pDrop.close();
    }

    //
    // Two below aren't exactly desirable, but confirming expected behavior
    //

    @Test
    public void createTableInvalidates() throws Exception {
        PreparedStatement pScan = newScan();
        PreparedStatement pCreate = newCreateTable();
        assertEquals("Row count from scan1", ROW_COUNT, countRows(pScan.executeQuery()));
        int count = pCreate.executeUpdate();
        assertEquals("Count from create table", 0, count);
        expectStale(pScan);
        pScan.close();
        pCreate.close();
    }

    @Test
    public void createIndexInvalidates() throws Exception {
        PreparedStatement pScan = newScan();
        PreparedStatement pCreate = newCreateIndex();
        assertEquals("Row count from scan1", ROW_COUNT, countRows(pScan.executeQuery()));
        int count = pCreate.executeUpdate();
        assertEquals("Count from create index", 0, count);
        expectStale(pScan);
        pScan.close();
        pCreate.close();
    }

    @Test
    public void fetchSize() throws Exception {
        boolean ac = getConnection().getAutoCommit();
        getConnection().setAutoCommit(false);
        PreparedStatement p = newScan();
        ResultSet rs = p.executeQuery();
        List<List<Object>> rows = listRows(rs, 2);
        p.setFetchSize(2);
        rs = p.executeQuery();
        assertEquals("Rows with fetch size 2", rows, listRows(rs, 2));
        getConnection().setAutoCommit(ac);
    }
}
