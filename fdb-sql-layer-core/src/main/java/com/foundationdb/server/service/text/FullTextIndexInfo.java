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
package com.foundationdb.server.service.text;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.*;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.NoSuchIndexException;
import com.foundationdb.server.error.NoSuchTableException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class FullTextIndexInfo
{
    private final FullTextIndexShared shared;
    private FullTextIndex index;
    private Schema schema;
    private TableRowType indexedRowType;
    private HKeyRowType hKeyRowType;
    private Map<Column,IndexedField> fieldsByColumn;
    private Map<RowType,List<IndexedField>> fieldsByRowType;
    private String defaultFieldName;
    private Operator plan;
    
    public FullTextIndexInfo(FullTextIndexShared shared) {
        this.shared = shared;
    }

    public void init(AkibanInformationSchema ais) {
        IndexName name = shared.getName();
        Table table = ais.getTable(name.getFullTableName());
        if (table == null) {
            throw new NoSuchTableException(name.getFullTableName());
        }
        index = table.getFullTextIndex(name.getName());
        if (index == null) {
            NoSuchIndexException ret =  new NoSuchIndexException(name.getName());
            ret.printStackTrace();
            throw ret;
        }
        schema = SchemaCache.globalSchema(ais);
        indexedRowType = schema.tableRowType(table);
        hKeyRowType = schema.newHKeyRowType(table.hKey());
        fieldsByColumn = new HashMap<>(index.getKeyColumns().size());
        for (IndexColumn indexColumn : index.getKeyColumns()) {
            Column column = indexColumn.getColumn();
            IndexedField indexedField = new IndexedField(column);
            fieldsByColumn.put(column, indexedField);
            if (defaultFieldName == null) {
                defaultFieldName = indexedField.getName();
            }
        }
        fieldsByRowType = new HashMap<>();
        for (Map.Entry<Column,IndexedField> entry : fieldsByColumn.entrySet()) {
            TableRowType rowType = schema.tableRowType(entry.getKey().getTable());
            List<IndexedField> fields = fieldsByRowType.get(rowType);
            if (fields == null) {
                fields = new ArrayList<>();
                fieldsByRowType.put(rowType, fields);
            }
            fields.add(entry.getValue());
        }
        
        plan = computePlan();
    }

    public FullTextIndex getIndex() {
        return index;
    }

    public Schema getSchema() {
        return schema;
    }

    public TableRowType getIndexedRowType() {
        return indexedRowType;
    }

    public HKeyRowType getHKeyRowType() {
        return hKeyRowType;
    }

    public Map<Column,IndexedField> getFieldsByColumn() {
        return fieldsByColumn;
    }

    public Map<RowType,List<IndexedField>> getFieldsByRowType() {
        return fieldsByRowType;
    }

    public Set<String> getCasePreservingFieldNames() {
        Set<String> result = new HashSet<>();
        for (IndexedField field : fieldsByColumn.values()) {
            if (field.isCasePreserving()) {
                result.add(field.getName());
            }
        }
        return result;
    }

    public String getDefaultFieldName() {
        return defaultFieldName;
    }

    public Set<RowType> getRowTypes() {
        Set<RowType> rowTypes = new HashSet<>(fieldsByRowType.keySet());
        rowTypes.add(indexedRowType);
        return rowTypes;
    }

    public Operator fullScan() {
        Operator plan = API.groupScan_Default(indexedRowType.table().getGroup());
        Set<RowType> rowTypes = getRowTypes();
        plan = API.filter_Default(plan, rowTypes);
        return plan;
    }
    
    
    
    private Operator computePlan()
    {
        Operator ret = null;

        Group group = indexedRowType.table().getGroup();
        Set<TableRowType> ancestors = new HashSet<>();
        boolean hasDesc = false;

        for (IndexColumn ic : index.getKeyColumns())
        {
            Table colTable = ic.getColumn().getTable();

            if (!hasDesc && !colTable.equals(indexedRowType.table()))
                // if any column in the index def belongs to a table
                // that is a descendant of this indexed row's table
                // (meaning this indexed row has descendant(s))
                hasDesc = colTable.isDescendantOf(indexedRowType.table());
            
            // if the indexed table is a child of this column's table
            // (meaning this indexed row has parent(s))
            // collect all ancestor's rowtype
            if (indexedRowType.table().isDescendantOf(colTable))
                ancestors.add(schema.tableRowType(colTable));
        }

        if (hasDesc)
        {
            ancestors.remove(indexedRowType);

            ret = API.branchLookup_Nested(group, 
                                           hKeyRowType,
                                           indexedRowType,
                                           API.InputPreservationOption.DISCARD_INPUT,
                                           0);
            if (!ancestors.isEmpty())
            {
                
                ret = API.groupLookup_Default(ret,
                                              group,
                                              indexedRowType, 
                                              ancestors, 
                                              API.InputPreservationOption.KEEP_INPUT,
                                              1);
            }
        }
        else
        {
            // has at least one ancestor (which is itself)
            ancestors.add(indexedRowType);
            ret = API.ancestorLookup_Nested(group,
                                            hKeyRowType,
                                            ancestors,
                                            0, 1);
        }
          
        return ret;
    }
    /**
     * @return the operator plan to get to every row related to this index row
     */
    public Operator getOperator()
    {
        return plan;
    }

    public Analyzer getAnalyzer() {
        Analyzer analyzer;
        synchronized (shared) {
            analyzer = shared.getAnalyzer();
            if (analyzer == null) {
                analyzer = new SelectiveCaseAnalyzer(shared.getCasePreservingFieldNames());
            }
        }
        return analyzer;
    }

    public StandardQueryParser getParser() {
        StandardQueryParser parser;
        synchronized (shared) {
            parser = shared.getParser();
            if (parser == null) {
                parser = new StandardQueryParser(getAnalyzer());
            }
            shared.setParser(parser);
        }
        return parser;
    }

    protected Searcher getSearcher() throws IOException {
        Searcher searcher;
        synchronized (shared) {
            searcher = shared.getSearcher();
            if (searcher == null) {
                searcher = new Searcher(shared, getAnalyzer());
            }
            shared.setSearcher(searcher);
        }
        return searcher;
    }

    public Indexer getIndexer() throws IOException {
        Indexer indexer;
        synchronized (shared) {
            indexer = shared.getIndexer();
            if (indexer == null) {
                indexer = new Indexer(shared, getAnalyzer());
                shared.setIndexer(indexer);
            }
        }
        return indexer;
    }

    public void deletePath() {
        File path = shared.getPath();
        // no doc to delete
        if (!path.exists() || path.listFiles() == null)
            return;
        for (File f : path.listFiles()) {
            f.delete();
        }
        path.delete();
    }

    public void commitIndexer() throws IOException {
        shared.getIndexer().getWriter().commit();
    }

    public void rollbackIndexer() throws IOException {
        synchronized (shared) {
            Indexer indexer = shared.getIndexer();
            if(indexer != null) {
                try {
                    indexer.getWriter().rollback();
                } finally {
                    // Rollback causes the writer to be closed. Always get rid of it.
                    shared.setIndexer(null);
                }
            }
        }
    }

    public void close() throws IOException {
        shared.close();
    }
}
