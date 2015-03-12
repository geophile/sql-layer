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
package com.foundationdb.server.service.is;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.virtualadapter.BasicFactoryBase;
import com.foundationdb.qp.virtualadapter.VirtualGroupCursor.GroupScan;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.store.SchemaManager;

public class SchemaTablesService {
    
    static final String SCHEMA_NAME = TableName.INFORMATION_SCHEMA;
    public static final int YES_NO_MAX = 3;
    public static final int DESCRIPTOR_MAX = 32;
    public static final int IDENT_MAX = 128;
    public static final int PATH_MAX = 1024;
    protected final SchemaManager schemaManager;

    public SchemaTablesService (SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }
    
    protected void attach(AkibanInformationSchema ais, TableName name, Class<? extends BasicFactoryBase> clazz) {
        Table table = ais.getTable(name);
        assert table != null;
        final BasicFactoryBase factory;
        try {
            factory = clazz.getConstructor(this.getClass(), TableName.class).newInstance(this, name);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        schemaManager.registerVirtualTable(table, factory);
    }
    
    protected void attach (AkibanInformationSchema ais, TableName name, BasicFactoryBase factory) {
        Table table = ais.getTable(name);
        assert table != null;
        schemaManager.registerVirtualTable(table, factory);
    }

    protected void attach (Table table, BasicFactoryBase factory) {
        schemaManager.registerVirtualTable(table, factory);
    }
    
    protected abstract class BaseScan implements GroupScan {
        final RowType rowType;
        long rowCounter = 0;
        
        public BaseScan (RowType rowType) {
            this.rowType = rowType;
        }
        
        @Override
        public void close() {
        }
        
    } 
}
