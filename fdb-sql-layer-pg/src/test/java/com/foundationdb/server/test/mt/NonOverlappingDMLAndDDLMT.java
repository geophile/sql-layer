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

import com.foundationdb.server.service.is.BasicInfoSchemaTablesService;
import com.foundationdb.server.service.is.BasicInfoSchemaTablesServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class NonOverlappingDMLAndDDLMT extends PostgresMTBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(BasicInfoSchemaTablesService.class, BasicInfoSchemaTablesServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    private static class SelectThread extends QueryThread
    {
        private final String schema;
        private final String table;

        public SelectThread(Connection conn, String schema, String table) throws SQLException {
            super("Select", schema, conn);
            this.schema = schema;
            this.table = table;
        }

        @Override
        protected int getLoopCount() {
            return 50;
        }

        @Override
        protected String[] getQueries() {
            return new String[] {
                "SELECT COUNT(*) FROM "+schema+"."+table,
                "SELECT * FROM "+schema+"."+table,
            };
        }
    }

    private static class DMLThread extends QueryThread
    {
        private final String schema;
        private final String table;

        public DMLThread(Connection conn, String schema, String table) throws SQLException {
            super("DML", schema, conn);
            this.schema = schema;
            this.table = table;
        }

        @Override
        protected int getLoopCount() {
            return 25;
        }

        @Override
        protected String[] getQueries() {
            return new String[] {
                "INSERT INTO "+schema+"."+table+" VALUES (10, 'ten')",
                "UPDATE "+schema+"."+table+" SET v='net' WHERE id=10",
                "DELETE FROM "+schema+"."+table+" WHERE id=10",
            };
        }
    }

    private static class DDLThread extends QueryThread
    {
        public DDLThread(Connection conn) throws SQLException {
            super("DDL", "test", conn);
        }

        @Override
        protected int getLoopCount() {
            return 25;
        }

        @Override
        protected String[] getQueries() {
            return new String[] {
                "CREATE TABLE t(ID INT NOT NULL PRIMARY KEY)",
                "DROP TABLE t"
            };
        }
    }

    private static class onlineDDLThread extends QueryThread 
    {
        public onlineDDLThread (Connection conn) throws SQLException {
            super ("onlineDDL", "a_schema", conn);
        }
        
        @Override
        protected int getLoopCount() {
            return 25;
        }
        
        @Override 
        protected String[] getQueries() {
            return new String[] {
                    "ALTER TABLE a_schema.t ADD COLUMN x INT",
                    "ALTER TABLE a_schema.t DROP COLUMN x"
            };
        }
    }
    @Test
    public void selectInfoSchema() throws SQLException {
        List<QueryThread> threads = Arrays.asList(
            new SelectThread(createConnection(), "information_schema", "tables"),
            new DDLThread(createConnection())
        );
        ThreadHelper.runAndCheck(30000, threads);
    }

    @Test
    public void selectRealTable() throws SQLException {
        String schema = "a_schema";
        String table = "t";
        int tid = createTable(schema, table, "id INT NOT NULL PRIMARY KEY, v VARCHAR(32)");
        for(int i = 1; i <= 5; ++i) {
            writeRow(tid, i, Integer.toString(i));
        }
        List<QueryThread> threads = Arrays.asList(
            new SelectThread(createConnection(), schema, table),
            new DDLThread(createConnection())
        );
        ThreadHelper.runAndCheck(30000, threads);
    }

    @Test
    public void dmlRealTable() throws SQLException {
        String schema = "a_schema";
        String table = "t";
        int tid = createTable(schema, table, "id INT NOT NULL PRIMARY KEY, v VARCHAR(32)");
        for(int i = 1; i <= 5; ++i) {
            writeRow(tid, i, Integer.toString(i));
        }
        List<QueryThread> threads = Arrays.asList(
            new DMLThread(createConnection(), schema, table),
            new DDLThread(createConnection())
        );
        ThreadHelper.runAndCheck(30000, threads);
    }
    
    @Test
    public void ddlOnlineTable() throws SQLException {
        int tid = createTable("a_schema", "t", "id INT NOT NULL PRIMARY KEY, v VARCHAR(32)");
        for (int i = 1; i <= 5; i++) {
            writeRow(tid, i, Integer.toString(i));
        }
        
        List<QueryThread> threads = Arrays.asList(
                new SelectThread (createConnection(), "information_schema", "tables"),
                new onlineDDLThread(createConnection())
                );
        ThreadHelper.runAndCheck(30000, threads);
    }
    
    @Test
    public void ddlOnlineSelectTable() throws SQLException {
        int tid = createTable("a_schema", "t", "id INT NOT NULL PRIMARY KEY, v VARCHAR(32)");
        for (int i = 1; i <= 5; i++) {
            writeRow(tid, i, Integer.toString(i));
        }
        
        List<QueryThread> threads = Arrays.asList(
                new SelectThread (createConnection(), "a_schema", "t"),
                new onlineDDLThread(createConnection())
                );
        ThreadHelper.runAndCheck(30000, threads);
        
    }
}
