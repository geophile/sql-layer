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
package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.FromTable;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SQLParserContext;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.Visitable;
import com.foundationdb.sql.parser.Visitor;
import com.foundationdb.sql.views.ViewDefinition;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.TableName;

import com.foundationdb.server.error.SQLParserInternalException;

import java.util.*;

public class AISViewDefinition extends ViewDefinition
{
    private Map<TableName,Collection<String>> tableColumnReferences;

    public AISViewDefinition(String sql, SQLParser parser)
            throws StandardException {
        super(sql, parser);
    }    

    public AISViewDefinition(StatementNode parsed, SQLParserContext parserContext)
            throws StandardException {
        super(parsed, parserContext);
    }
    
    public Map<TableName,Collection<String>> getTableColumnReferences() {
        if (tableColumnReferences == null) {
            ReferenceCollector collector = new ReferenceCollector();
            try {
                getSubquery().accept(collector);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            tableColumnReferences = collector.references;
        }
        return tableColumnReferences;
    }

    static class ReferenceCollector implements Visitor {
        Map<TableName,Collection<String>> references = new HashMap<>();
        
        @Override
        public Visitable visit(Visitable node) throws StandardException {
            if (node instanceof FromTable) {
                TableBinding tableBinding = (TableBinding)((FromTable)node).getUserData();
                if (tableBinding != null) {
                    Columnar table = tableBinding.getTable();
                    if (!references.containsKey(table.getName())) {
                        references.put(table.getName(), new HashSet<String>());
                    }
                }
            }
            else if (node instanceof ColumnReference) {
                ColumnBinding columnBinding = (ColumnBinding)((ColumnReference)node).getUserData();
                if (columnBinding != null) {
                    Column column = columnBinding.getColumn();
                    if (column != null) {
                        Columnar table = column.getColumnar();
                        Collection<String> entry = references.get(table.getName());
                        if (entry == null) {
                            entry = new HashSet<>();
                            references.put(table.getName(), entry);
                        }
                        entry.add(column.getName());
                    }
                }
            }
            return node;
        }

        @Override
        public boolean visitChildrenFirst(Visitable node) {
            return true;
        }

        @Override
        public boolean stopTraversal() {
            return false;
        }

        @Override
        public boolean skipChildren(Visitable node) throws StandardException {
            return false;
        }
    }
}
