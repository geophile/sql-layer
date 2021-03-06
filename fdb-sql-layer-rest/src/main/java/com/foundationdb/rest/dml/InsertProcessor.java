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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.externaldata.TableRowTracker;
import com.fasterxml.jackson.databind.JsonNode;
import com.foundationdb.server.types.value.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.server.error.FKValueMismatchException;
import com.foundationdb.server.service.externaldata.JsonRowWriter;
import com.foundationdb.server.service.externaldata.JsonRowWriter.WriteCapturePKRow;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.util.AkibanAppender;

public class InsertProcessor extends DMLProcessor {
    private InsertGenerator insertGenerator;
    private FormatOptions options;
    private static final Logger LOG = LoggerFactory.getLogger(InsertProcessor.class);

    public InsertProcessor (
            Store store, SchemaManager schemaManager,
            TypesRegistryService typesRegistryService,
            FormatOptions options) {
        super (store, schemaManager, typesRegistryService);
        this.options = options;
    }
    
    private static final CacheValueGenerator<InsertGenerator> CACHED_INSERT_GENERATOR =
            new CacheValueGenerator<InsertGenerator>() {
                @Override
                public InsertGenerator valueFor(AkibanInformationSchema ais) {
                    return new InsertGenerator(ais);
                }
            };

    public String processInsert(Session session, AkibanInformationSchema ais, TableName rootTable, JsonNode node) {
        ProcessContext context = new ProcessContext ( ais, session, rootTable);
        insertGenerator = getGenerator(CACHED_INSERT_GENERATOR, context);
        StringBuilder builder = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(builder);
        processContainer (node, appender, context);
        return appender.toString();
    }
    
    private void processContainer (JsonNode node, AkibanAppender appender, ProcessContext context) {
        boolean first = true;
        Map<Column, ValueSource> pkValues = null;
        
        if (node.isObject()) {
            processTable (node, appender, context);
        } else if (node.isArray()) {
            appender.append('[');
            for (JsonNode arrayElement : node) {
                if (first) { 
                    pkValues = context.pkValues;
                    first = false;
                } else {
                    appender.append(',');
                }
                if (arrayElement.isObject()) {
                    processTable (arrayElement, appender, context);
                    context.pkValues = pkValues;
                    context.queryBindings.clear();
                    context.allValues.clear();
                }
                // else throw Bad Json Format Exception
            }
            appender.append(']');
        } // else throw Bad Json Format Exception
        
    }
    
    private void processTable (JsonNode node, AkibanAppender appender, ProcessContext context) {
        // Pass one, insert fields from the table
        Iterator<Entry<String,JsonNode>> i = node.fields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field = i.next();
            if (field.getValue().isValueNode()) {
                Column column = getColumn(context.table, field.getKey());
                context.allValues.put(column, field.getValue().isNull() ? null : field.getValue().asText());
            }
        }
        runInsert(context, appender);
        boolean first = true;
        // pass 2: insert the child nodes
        i = node.fields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field = i.next();
            if (field.getValue().isContainerNode()) {
                if (first) {
                    first = false;
                    // Delete the closing } for the object
                    StringBuilder builder = (StringBuilder)appender.getAppendable();
                    builder.deleteCharAt(builder.length()-1);
                } 
                TableName tableName = TableName.parse(context.tableName.getSchemaName(), field.getKey());
                ProcessContext newContext = new ProcessContext(context.ais(), context.session, tableName);
                newContext.pkValues = context.pkValues;
                appender.append(",\"");
                appender.append(newContext.table.getNameForOutput());
                appender.append("\":");
                processContainer (field.getValue(), appender, newContext);
            }
        }
        // we appended at least one sub-object, so replace the object close brace. 
        if (!first) {
            appender.append('}');
        }
    }
     private void runInsert(ProcessContext context, AkibanAppender appender) {
        assert context != null : "Bad Json format";
        LOG.trace("Insert row into: {}, values {}", context.tableName, context.queryContext);
        // Fill in parent columns if this is a child table
        if(context.pkValues != null && context.table.getParentJoin() != null) {
            Join join = context.table.getParentJoin();
            for (Entry<Column, ValueSource> entry : context.pkValues.entrySet()) {
                Column parentCol = entry.getKey();
                Column childCol = join.getMatchingChild(parentCol);
                String fkValue = valueToString(entry.getValue());
                String curValue = context.allValues.get(childCol);
                if(curValue == null) {
                    context.allValues.put(childCol, fkValue);
                } else if(!fkValue.equals(curValue)) {
                    throw new FKValueMismatchException(join.getMatchingChild(entry.getKey()).getName());
                }
            }
        }
        Operator insert = insertGenerator.create(context.allValues, context.table.getName());
        Cursor cursor = API.cursor(insert, context.queryContext, context.queryBindings);
        JsonRowWriter writer = new JsonRowWriter(new TableRowTracker(context.table, 0));
        WriteCapturePKRow rowWriter = new WriteCapturePKRow();
        writer.writeRows(cursor, appender, context.anyUpdates ? "\n" : "", rowWriter, options);
        context.pkValues = rowWriter.getPKValues();
        context.anyUpdates = true;
    }
    
    private String valueToString(ValueSource value) {
        AkibanAppender appender = AkibanAppender.of(new StringBuilder());
        value.getType().format(value, appender);
        return appender.toString();
    }
}
