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

import com.fasterxml.jackson.databind.JsonNode;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.service.TypesRegistryService;

public class UpdateProcessor extends DMLProcessor {

    private final DeleteProcessor deleteProcessor;
    private final InsertProcessor insertProcessor;

    public UpdateProcessor(Store store, SchemaManager schemaManager,
            TypesRegistryService typesRegistryService,
            DeleteProcessor deleteProcessor,
            InsertProcessor insertProcessor) {
        super(store, schemaManager, typesRegistryService);
        this.deleteProcessor = deleteProcessor;
        this.insertProcessor = insertProcessor;
    }

    public String processUpdate (Session session, AkibanInformationSchema ais, TableName tableName, String values, JsonNode node) {
        deleteProcessor.processDelete(session, ais, tableName, values);
        return insertProcessor.processInsert(session, ais, tableName, node);
        
    }
}
