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

import com.foundationdb.qp.row.Row;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.util.Collections;
import java.util.Map;

public class PostgresServerCacheIT extends PostgresServerFilesITBase
{
    public static final String QUERY = "SELECT id FROM t1 WHERE id = %d";
    public static final String PQUERY = "SELECT id FROM t1 WHERE id = ?";
    public static final int NROWS = 100;
    public static final String CAPACITY = "10";

    private int hitsBase;
    private int missesBase;
    
    @Override
    protected Map<String, String> startupConfigProperties() {
        return Collections.singletonMap("fdbsql.postgres.statementCacheCapacity", CAPACITY);
    }

    @Before
    public void createData() throws Exception {
        int tid = createTable(SCHEMA_NAME, "t1", "id int not null primary key");
        Row[] rows = new Row[NROWS];
        for (int i = 0; i < NROWS; i++) {
            rows[i] = row(tid, i);
        }
        writeRows(rows);
        hitsBase = server().getStatementCacheHits();
        missesBase = server().getStatementCacheMisses();
    }

    @Test
    public void testRepeated() throws Exception {
        Statement stmt = getConnection().createStatement();
        for (int i = 0; i < 1000; i++) {
            query(stmt, i / NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 990, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 10, server().getStatementCacheMisses() - missesBase);
    }

    @Test
    public void testSequential() throws Exception {
        Statement stmt = getConnection().createStatement();
        for (int i = 0; i < 1000; i++) {
            query(stmt, i % NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 0, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 1000, server().getStatementCacheMisses() - missesBase);
    }

    @Test
    public void testPreparedRepeated() throws Exception {
        PreparedStatement stmt = getConnection().prepareStatement(PQUERY);
        for (int i = 0; i < 1000; i++) {
            pquery(stmt, i / NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 4, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 1, server().getStatementCacheMisses() - missesBase);
    }
    
    @Test
    public void testPreparedSequential() throws Exception {
        PreparedStatement stmt = getConnection().prepareStatement(PQUERY);
        for (int i = 0; i < 1000; i++) {
            pquery(stmt, i % NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 4, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 1, server().getStatementCacheMisses() - missesBase);
        
    }
    
    protected void query(Statement stmt, int n) throws Exception {
        ResultSet rs = stmt.executeQuery(String.format(QUERY, n));
        if (rs.next()) {
            assertEquals("Query result matches", n, rs.getInt(1));
        }
        else {
            fail("No query results");
        }
    }
    
    protected void pquery (PreparedStatement stmt, int n) throws Exception {
        stmt.setInt(1, n);
        stmt.execute();
        ResultSet rs = stmt.getResultSet();
        if (rs.next()) {
            assertEquals("Query Result Matches", n, rs.getInt(1));
        } else {
            fail ("No Query results");
            
        }
    }
}
