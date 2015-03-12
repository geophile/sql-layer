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
package com.foundationdb.rest.dml;

import java.util.HashMap;
import java.util.Map;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.ProtectedTableDDLException;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.value.ValueSource;

public abstract class DMLProcessor {

    private final Store store;
    private final SchemaManager schemaManager;
    private final TypesRegistryService registryService;
    
    public DMLProcessor(Store store, SchemaManager schemaManager,
                        TypesRegistryService typesRegistryService) {
        this.store = store;
        this.schemaManager = schemaManager;
        this.registryService = typesRegistryService;
    }

    protected Column getColumn (Table table, String field) {
        Column column = table.getColumn(field);
        if (column == null) {
            throw new NoSuchColumnException(field);
        }
        return column;
    }

    protected <T extends OperatorGenerator> T getGenerator(CacheValueGenerator<T> generator, ProcessContext context) {
        T gen = context.ais().getCachedValue(this, generator);
        gen.setTypesRegistry(registryService);
        gen.setTypesTranslator(context.typesTranslator);
        return gen;
    }
    
    protected TypesTranslator getTypesTranslator() {
        return schemaManager.getTypesTranslator();
    }

    public class ProcessContext {
        public TableName tableName;
        public Table table;
        public QueryContext queryContext;
        public QueryBindings queryBindings;
        public Session session;
        public Map<Column, ValueSource> pkValues;
        public Map<Column, String> allValues;
        public boolean anyUpdates;
        private final AkibanInformationSchema ais;
        public final TypesTranslator typesTranslator;

        public ProcessContext (AkibanInformationSchema ais, Session session, TableName tableName) {
            this.tableName = tableName;
            this.ais = ais;
            this.session = session;
            this.typesTranslator = getTypesTranslator();
            this.table = getTable();
            this.queryContext = new RestQueryContext(getAdapter());
            this.queryBindings = queryContext.createBindings();
            allValues = new HashMap<>();
        }
     
        protected AkibanInformationSchema ais() {
            return ais;
        }
        
        private StoreAdapter getAdapter() {
            // no writing to the virtual tables.
            if (table.isVirtual())
                throw new ProtectedTableDDLException (table.getName());
            return store.createAdapter(session);
        }

        private Table getTable () {
            Table table = ais.getTable(tableName);
            if (table == null) {
                throw new NoSuchTableException(tableName.getSchemaName(), tableName.getTableName());
            } else if (table.isProtectedTable()) {
                throw  new ProtectedTableDDLException (table.getName());
            }
            return table;
        }
    }
}
