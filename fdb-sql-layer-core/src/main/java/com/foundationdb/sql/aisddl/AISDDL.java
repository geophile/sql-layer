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
package com.foundationdb.sql.aisddl;

import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.CreateAliasNode;
import com.foundationdb.sql.parser.CreateIndexNode;
import com.foundationdb.sql.parser.CreateSequenceNode;
import com.foundationdb.sql.parser.CreateTableNode;
import com.foundationdb.sql.parser.CreateSchemaNode;
import com.foundationdb.sql.parser.CreateViewNode;
import com.foundationdb.sql.parser.DropAliasNode;
import com.foundationdb.sql.parser.DropGroupNode;
import com.foundationdb.sql.parser.DropIndexNode;
import com.foundationdb.sql.parser.DropSequenceNode;
import com.foundationdb.sql.parser.DropTableNode;
import com.foundationdb.sql.parser.DropSchemaNode;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.DropViewNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.RenameNode;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerSession;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.error.UnsupportedTriggerException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.common.types.TypesTranslator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AISDDL
{
    private static final Logger logger = LoggerFactory.getLogger(AISDDL.class);

    private AISDDL() {}
    
    public static void execute(DDLStatementNode ddl, String sql, 
                               ServerQueryContext<?> context) {
        ServerSession server = context.getServer();
        AkibanInformationSchema ais = server.getAIS();
        String schema = server.getDefaultSchemaName();
        logger.info("DDL in {}: {}", schema, sql);
        DDLFunctions ddlFunctions = server.getDXL().ddlFunctions();
        Session session = server.getSession();
        switch (ddl.getNodeType()) {
        case NodeTypes.CREATE_SCHEMA_NODE:
            SchemaDDL.createSchema(ais, schema, (CreateSchemaNode)ddl, context);
            return;
        case NodeTypes.DROP_SCHEMA_NODE:
            SchemaDDL.dropSchema(ddlFunctions, session, (DropSchemaNode)ddl, context);
            return;
        case NodeTypes.CREATE_TABLE_NODE:
            TableDDL.createTable(ddlFunctions, session, schema, (CreateTableNode)ddl, context);
            return;
        case NodeTypes.DROP_TABLE_NODE:
            TableDDL.dropTable(ddlFunctions, session, schema, (DropTableNode)ddl, context);
            return;
        case NodeTypes.DROP_GROUP_NODE:
            TableDDL.dropGroup(ddlFunctions, session, schema, (DropGroupNode)ddl, context);
            return;
        case NodeTypes.CREATE_VIEW_NODE:
            ViewDDL.createView(ddlFunctions, session, schema, (CreateViewNode)ddl,
                               server.getBinderContext(), context, server);
            return;
        case NodeTypes.DROP_VIEW_NODE:
            ViewDDL.dropView(ddlFunctions, session, schema, (DropViewNode)ddl,
                             server.getBinderContext(), context);
            return;
        case NodeTypes.CREATE_INDEX_NODE:
            IndexDDL.createIndex(ddlFunctions, session, schema, (CreateIndexNode)ddl, context);
            return;
        case NodeTypes.DROP_INDEX_NODE:
            IndexDDL.dropIndex(ddlFunctions, session, schema, (DropIndexNode)ddl, context);
            return;
        case NodeTypes.ALTER_TABLE_NODE:
            AlterTableDDL.alterTable(ddlFunctions, server.getDXL().dmlFunctions(), session, schema, (AlterTableNode)ddl, context);
            return;
        case NodeTypes.RENAME_NODE:
            switch (((RenameNode)ddl).getRenameType()) {
            case INDEX:
                IndexDDL.renameIndex(ddlFunctions, session, schema, (RenameNode)ddl);
                return;
            case TABLE:
                TableDDL.renameTable(ddlFunctions, session, schema, (RenameNode)ddl);
                return;
            }
            break;
        case NodeTypes.CREATE_SEQUENCE_NODE:
            SequenceDDL.createSequence(ddlFunctions, session, schema, (CreateSequenceNode)ddl);
            return;
        case NodeTypes.DROP_SEQUENCE_NODE:
            SequenceDDL.dropSequence(ddlFunctions, session, schema, (DropSequenceNode)ddl, context);
            return;
        case NodeTypes.CREATE_ALIAS_NODE:
            switch (((CreateAliasNode)ddl).getAliasType()) {
            case PROCEDURE:
            case FUNCTION:
                RoutineDDL.createRoutine(ddlFunctions, server.getRoutineLoader(), session, schema, (CreateAliasNode)ddl);
                return;
            }
            break;
        case NodeTypes.DROP_ALIAS_NODE:
            switch (((DropAliasNode)ddl).getAliasType()) {
            case PROCEDURE:
            case FUNCTION:
                RoutineDDL.dropRoutine(ddlFunctions, server.getRoutineLoader(), session, schema, (DropAliasNode)ddl, context);
                return;
            }
            break;
        case NodeTypes.CREATE_TRIGGER_NODE:
            throw new UnsupportedTriggerException();
        }
        throw new UnsupportedSQLException(ddl.statementToString(), ddl);
    }
}
