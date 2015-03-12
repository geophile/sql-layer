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

import java.util.List;

import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.service.TypesRegistryService;

public class DeleteProcessor extends DMLProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteProcessor.class);
    private DeleteGenerator deleteGenerator;
    
    public DeleteProcessor (
            Store store, SchemaManager schemaManager,
            TypesRegistryService typesRegistryService) {
        super (store, schemaManager, typesRegistryService);
    }

    private static final CacheValueGenerator<DeleteGenerator> CACHED_DELETE_GENERATOR =
            new CacheValueGenerator<DeleteGenerator>() {
                @Override
                public DeleteGenerator valueFor(AkibanInformationSchema ais) {
                    return new DeleteGenerator(ais);
                }
            };

    public void processDelete (Session session, AkibanInformationSchema ais, TableName tableName, String identifiers) {
        ProcessContext context = new ProcessContext (ais, session, tableName);
        
        deleteGenerator = getGenerator (CACHED_DELETE_GENERATOR, context);

        Index pkIndex = context.table.getPrimaryKeyIncludingInternal().getIndex();
        List<List<Object>> pks = PrimaryKeyParser.parsePrimaryKeys(identifiers, pkIndex);
        
        Cursor cursor = null;

        try {
            Operator delete = deleteGenerator.create(tableName);
            cursor = API.cursor(delete, context.queryContext, context.queryBindings);

            for (List<Object> key : pks) {
                for (int i = 0; i < key.size(); i++) {
                    ValueSource value = ValueSources.fromObject(key.get(i));
                    context.queryBindings.setValue(i, value);
                }
    
                cursor.openTopLevel();
                Row row;
                while ((row = cursor.next()) != null) {
                    // Do Nothing - the act of reading the cursor 
                    // does the delete row processing.
                    // TODO: Check that we got 1 row through.
                }
                cursor.closeTopLevel();
            }
        } finally {
            if (cursor != null && !cursor.isClosed())
                cursor.close();

        }
    }
}
