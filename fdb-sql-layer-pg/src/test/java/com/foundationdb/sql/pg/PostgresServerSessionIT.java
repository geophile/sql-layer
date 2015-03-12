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

import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.api.DDLFunctions;

public class PostgresServerSessionIT extends PostgresServerFilesITBase {

    @Before
    public void createSimpleSchema() throws Exception {
        String sqlCreate = "CREATE TABLE fake.T1 (c1 integer not null primary key)";
        getConnection().createStatement().execute(sqlCreate);
    }
    
    @After
    public void dontLeaveConnection() throws Exception {
        // Tests change current schema. Easiest not to reuse.
        forgetConnection();
    }

    @Test
    public void createNewTable() throws Exception {
        String create  = "CREATE TABLE t1 (c2 integer not null primary key)";
        getConnection().createStatement().execute(create);
        
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getTable("fake", "t1"));
        assertNotNull (ais.getTable(SCHEMA_NAME, "t1"));

    }

    @Test 
    public void goodUseSchema() throws Exception {
        String use = "SET SCHEMA fake";
        getConnection().createStatement().execute(use);
        
        String create = "CREATE TABLE t2 (c2 integer not null primary key)";
        getConnection().createStatement().execute(create);
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getTable("fake", "t2"));
        assertNotNull (ais.getTable("fake", "t1"));
    }
    
    @Test (expected=SQLException.class)
    public void badUseSchema() throws Exception {
        String use = "SET SCHEMA BAD";
        getConnection().createStatement().execute(use);
    }
    
    @Test 
    public void useUserSchema() throws Exception {
        String create  = "CREATE TABLE t1 (c2 integer not null primary key)";
        getConnection().createStatement().execute(create);
        
        create  = "CREATE TABLE auser.t1 (c4 integer not null primary key)";
        getConnection().createStatement().execute(create);
        
        getConnection().createStatement().execute("SET SCHEMA auser");
        
        getConnection().createStatement().execute("SET SCHEMA "+PostgresServerITBase.SCHEMA_NAME);
        
    }
    
    protected DDLFunctions ddlServer() {
        return serviceManager().getDXL().ddlFunctions();
    }

}
