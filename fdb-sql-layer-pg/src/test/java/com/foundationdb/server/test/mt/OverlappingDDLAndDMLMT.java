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
package com.foundationdb.server.test.mt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Test;

public class OverlappingDDLAndDMLMT extends PostgresMTBase {

    
    @Test
    public void statementOverlap() throws SQLException {
        Connection conn1 = createConnection(); 
        conn1.setAutoCommit(false);

        
        Connection conn2 = createConnection();
        conn2.setAutoCommit(true);
        
        PreparedStatement p1 = conn1.prepareStatement("SELECT * from information_schema.tables");
        conn1.commit();
        
        conn2.createStatement().execute("Create Table t (id int not null primary key, name varchar(32))");
        
        assertTrue (p1.execute());
    }
    
    @Test
    public void prepareOverlap() throws SQLException {
        Connection conn1 = createConnection(); 
        conn1.setAutoCommit(false);
       
        Connection conn2 = createConnection();
        conn2.setAutoCommit(true);
        
        conn1.createStatement().execute("PREPARE q1 as SELECT * from information_schema.tables");
        conn1.commit();
        
        conn2.createStatement().execute("Create Table t (id int not null primary key, name varchar(32))");
        
        try {
        assertTrue(!conn1.createStatement().execute("EXECUTE q1"));
        } catch (SQLException e) {
            assertEquals (e.getMessage(), "ERROR: Unusable prepared statement due to DDL after generation");
            assertEquals (e.getSQLState(), "0A50A");
        }
    }
    
    @Test
    public void prepareOverlapRollback() throws SQLException   {
        Connection conn1 = createConnection(); 
        conn1.setAutoCommit(false);
        
        Connection conn2 = createConnection();
        conn2.setAutoCommit(false);
        
        conn1.createStatement().execute("PREPARE q1 as SELECT * from information_schema.tables");
        conn1.commit();
        
        conn2.createStatement().execute("Create Table t (id int not null primary key, name varchar(32))");
        conn2.rollback();
        
        try {
        assertTrue(!conn1.createStatement().execute("EXECUTE q1"));
        } catch (SQLException e) {
            assertEquals (e.getMessage(), "ERROR: Unusable prepared statement due to DDL after generation");
            assertEquals (e.getSQLState(), "0A50A");
        }
        
    }
}
