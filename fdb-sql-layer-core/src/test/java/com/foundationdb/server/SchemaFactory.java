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
package com.foundationdb.server;

import com.foundationdb.ais.model.AISMerge;
import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.View;
import com.foundationdb.qp.virtualadapter.VirtualAdapter;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.SimpleTableStatusCache;
import com.foundationdb.server.TableStatus;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.api.ddl.DDLFunctionsMockBase;
import com.foundationdb.server.service.routines.MockRoutineLoader;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.aisddl.AlterTableDDL;
import com.foundationdb.sql.aisddl.IndexDDL;
import com.foundationdb.sql.aisddl.RoutineDDL;
import com.foundationdb.sql.aisddl.SequenceDDL;
import com.foundationdb.sql.aisddl.TableDDL;
import com.foundationdb.sql.aisddl.ViewDDL;
import com.foundationdb.sql.optimizer.AISBinderContext;
import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.CreateAliasNode;
import com.foundationdb.sql.parser.CreateIndexNode;
import com.foundationdb.sql.parser.CreateTableNode;
import com.foundationdb.sql.parser.CreateViewNode;
import com.foundationdb.sql.parser.CreateSequenceNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaFactory {
    private final static String DEFAULT_DEFAULT_SCHEMA = "test";
    private final String defaultSchema;

    public SchemaFactory() {
        this(DEFAULT_DEFAULT_SCHEMA);
    }
    
    public SchemaFactory(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public AkibanInformationSchema aisWithTableStatus(String... ddl) {
        AkibanInformationSchema ais = ais(ddl);
        buildTableStatusAndFieldAssociations(ais);
        return ais;
    }

    public AkibanInformationSchema ais(String... ddl) {
        DDLFunctions ddlFunctions = new CreateOnlyDDLMock();
        Session session = null;
        ddl(ddlFunctions, session, ddl);
        return ddlFunctions.getAIS(session);
    }

    public void ddl(DDLFunctions ddlFunctions, Session session, String... ddl){
        ddl(ddlFunctions, session, null, null, null, ddl);
    }
    public void ddl(DDLFunctions ddlFunctions, Session session,  List<DataTypeDescriptor> descriptors, List<String> columnNames,
                    ServerSession server, String... ddl){
        StringBuilder buffer = new StringBuilder();
        for (String line : ddl) {
            buffer.append(line);
        }
        String fullDDL = buffer.toString();
        SQLParser parser = new SQLParser();
        List<StatementNode> nodes;
        try {
            nodes = parser.parseStatements(fullDDL);
        } catch(StandardException e) {
            throw new RuntimeException(e);
        }
        for(StatementNode stmt : nodes) {
            if (stmt instanceof CreateTableNode) {
                if( descriptors != null && columnNames != null){
                    TableDDL.createTable(ddlFunctions, session, defaultSchema, (CreateTableNode) stmt, null, descriptors, columnNames, server);
                } else {
                    TableDDL.createTable(ddlFunctions, session, defaultSchema, (CreateTableNode) stmt, null);
                }
            } else if (stmt instanceof CreateIndexNode) {
                IndexDDL.createIndex(ddlFunctions, session, defaultSchema, (CreateIndexNode) stmt, null);
            } else if (stmt instanceof CreateViewNode) {
                ViewDDL.createView(ddlFunctions, session, defaultSchema, (CreateViewNode) stmt,
                                   new AISBinderContext(ddlFunctions.getAIS(session), defaultSchema), null, server);
            } else if (stmt instanceof CreateSequenceNode) {
                SequenceDDL.createSequence(ddlFunctions, session, defaultSchema, (CreateSequenceNode)stmt);
            } else if (stmt instanceof CreateAliasNode) {
                RoutineDDL.createRoutine(ddlFunctions, new MockRoutineLoader(), session, defaultSchema, (CreateAliasNode)stmt);
            } else if (stmt instanceof AlterTableNode) {
                AlterTableNode atNode = (AlterTableNode) stmt;
                assert !atNode.isTruncateTable() : "TRUNCATE not supported";
                AlterTableDDL.alterTable(ddlFunctions,
                                         null /* DMLFunctions */,
                                         session,
                                         defaultSchema,
                                         (AlterTableNode)stmt,
                                         null /*QueryContext*/);
            } else {
                throw new IllegalStateException("Unsupported StatementNode type: " + stmt);
            }
        }
    }

    public void buildTableStatusAndFieldAssociations(AkibanInformationSchema ais) {
        final Map<Table,Integer> ordinalMap = new HashMap<>();
        for(Group group : ais.getGroups().values()) {
            group.getRoot().visit(new AbstractVisitor() {
                int ordinal = 1;

                @Override
                public void visit(Table table) {
                    table.setOrdinal(ordinal++);
                    ordinalMap.put(table, table.getOrdinal());
                }
            });
        }
        SimpleTableStatusCache tableStatusCache = new SimpleTableStatusCache();
        for (final Table table : ais.getTables().values()) {
            // Field associations
            table.computeFieldAssociations(ordinalMap);
            // Table status
            final TableStatus status;
            if (table.isVirtual()) {
                status = tableStatusCache.getOrCreateVirtualTableStatus(table.getTableId(),
                                                                        VirtualAdapter.getFactory(table));
            } else {
                status = tableStatusCache.createTableStatus(table);
            }
            table.tableStatus(status);
        }

    }

    private static class CreateOnlyDDLMock extends DDLFunctionsMockBase {
        AkibanInformationSchema ais = new AkibanInformationSchema();

        @Override
        public void createTable(Session session, Table newTable) {
            AISMerge merge = AISMerge.newForAddTable(getAISCloner(), new DefaultNameGenerator(ais), ais, newTable);
            merge.merge();
            ais = merge.getAIS();
        }

        @Override
        public void createTable(Session session, Table newTable, String queryExpression, QueryContext context, ServerSession server) {
            AISMerge merge = AISMerge.newForAddTable(getAISCloner(), new DefaultNameGenerator(ais), ais, newTable);
            merge.merge();
            ais = merge.getAIS();
        }

        @Override
        public void createView(Session session, View view) {
            ais = AISMerge.mergeView(getAISCloner(), ais, view);
        }

        @Override
        public AkibanInformationSchema getAIS(Session session) {
            return ais;
        }

        @Override
        public void createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
            AISMerge merge = AISMerge.newForAddIndex(getAISCloner(), new DefaultNameGenerator(ais), ais);
            for(Index newIndex : indexesToAdd) {
                merge.mergeIndex(newIndex);
            }
            merge.merge();
            ais = merge.getAIS();
        }
        
        @Override
        public void createSequence(Session session, Sequence sequence) {
            AISMerge merge = AISMerge.newForOther(getAISCloner(), new DefaultNameGenerator(ais), ais);
            ais = merge.mergeSequence(sequence);
        }

        @Override
        public void createRoutine(Session session, Routine routine, boolean replaceExisting) {
            ais = AISMerge.mergeRoutine(getAISCloner(), ais, routine);
        }
    }
}
