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
package com.foundationdb.qp.virtualadapter;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.sql.ServerSessionITBase;

public class VirtualAdapterIT extends ServerSessionITBase {
    private static final TableName TEST_NAME = new TableName (TableName.INFORMATION_SCHEMA, "test");
    private SchemaManager schemaManager;
    private Table table;
    
    private void registerISTable(final Table table, final VirtualScanFactory factory) throws Exception {
        schemaManager.registerVirtualTable(table, factory);
    }

    @Before
    public void setUp() throws Exception {
        schemaManager = serviceManager().getSchemaManager();
    }

    @After
    public void unRegister() {
        if(ais().getTable(TEST_NAME) != null) {
            schemaManager.unRegisterVirtualTable(TEST_NAME);
        }
    }

    @Test
    public void insertFactoryTest() throws Exception {
  
        table = AISBBasedBuilder.create(ddl().getTypesTranslator()).table(TEST_NAME.getSchemaName(),TEST_NAME.getTableName()).colInt("c1").pk("c1").ais().getTable(TEST_NAME);
        VirtualScanFactory factory = new TestFactory (TEST_NAME);

        registerISTable(table, factory);
 
        Table table = ais().getTable(TEST_NAME);
        assertNotNull (table);
        assertTrue (table.isVirtual());
    }

    @Test
    public void testGetAdapter() throws Exception {
        table = AISBBasedBuilder.create(ddl().getTypesTranslator()).table(TEST_NAME.getSchemaName(),TEST_NAME.getTableName()).colInt("c1").pk("c1").ais().getTable(TEST_NAME);
        VirtualScanFactory factory = new TestFactory (TEST_NAME);
        registerISTable(table, factory);
        Table newtable = ais().getTable(TEST_NAME);
        
        TestSession sqlSession = new TestSession();

        StoreAdapter adapter = sqlSession.getStoreHolder().getAdapter(newtable);
        assertNotNull (adapter);
        assertTrue (adapter instanceof VirtualAdapter);
    }

    private class TestFactory implements VirtualScanFactory
    {
        
        private TableName name;
        public TestFactory (TableName name) {
            this.name = name;
        }

        @Override
        public VirtualGroupCursor.GroupScan getGroupScan(VirtualAdapter adaper, Group group) {
            return null;
        }

        @Override
        public TableName getName() {
            return name;
        }

        @Override
        public long rowCount(Session session) {
            return 0;
        }
    }
}
