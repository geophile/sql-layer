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
package com.foundationdb.sql.pg;

import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.sql.aisddl.AISDDL;
import com.foundationdb.sql.aisddl.TableDDL;
import com.foundationdb.sql.parser.*;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

/** SQL DDL statements. */
public class PostgresDDLStatement extends PostgresBaseStatement
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresDDLStatement.class);
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresDDLStatement: execute shared");

    private DDLStatementNode ddl;
    private String sql;
    private List<String> columnNames = null;
    private List<PostgresType> columnTypes = null;
    PostgresOperatorStatement opstmt;

    public PostgresDDLStatement(DDLStatementNode ddl, String sql) {
        this.sql = sql;
        this.ddl = ddl;
    }

    public PostgresDDLStatement(DDLStatementNode ddl, String sql, PostgresOperatorStatement opstmt) {
        this.sql = sql;
        this.ddl = ddl;
        this.opstmt = opstmt;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        if(opstmt != null) {
            opstmt.finishGenerating(server, sql, ((CreateTableNode) stmt).getQueryExpression(), params, paramTypes);
            columnNames = opstmt.getColumnNames();
            columnTypes = opstmt.getColumnTypes();
        }
        super.finishGenerating(server,sql, stmt, params, paramTypes);
        return this;
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
            if (params) {
                messenger.beginMessage(PostgresMessages.PARAMETER_DESCRIPTION_TYPE.code());
                messenger.writeShort(0);
                messenger.sendMessage();
            }
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public TransactionMode getTransactionMode() {
        if(opstmt != null){
            return TransactionMode.IMPLICIT_COMMIT_AND_NEW;
        }
        return TransactionMode.IMPLICIT_COMMIT;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

    @Override
    public boolean putInCache() {
        return false;
    }

    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.getSessionMonitor().countEvent(StatementTypes.DDL_STMT);
        PostgresMessenger messenger = server.getMessenger();
        //if this is a create table node with a query expression use special case
        if(ddl.getNodeType() == NodeTypes.CREATE_TABLE_NODE && ((CreateTableNode)ddl).getQueryExpression() != null){
            try{
                preExecute(context, DXLFunction.UNSPECIFIED_DDL_WRITE);
                String schema = server.getDefaultSchemaName();
                DDLFunctions ddlFunctions = server.getDXL().ddlFunctions();
                Session session = server.getSession();
                List<DataTypeDescriptor> descriptors = new ArrayList<>();
                for(PostgresType columnType: columnTypes){
                    descriptors.add(columnType.getType().dataTypeDescriptor());
                }

                TableDDL.createTable(ddlFunctions, session, schema, (CreateTableNode) ddl, context, descriptors, columnNames, server);
            }
            finally {
                postExecute(context, DXLFunction.UNSPECIFIED_DDL_WRITE);
            }
        }  else {
            try {
                preExecute(context, DXLFunction.UNSPECIFIED_DDL_WRITE);
                AISDDL.execute(ddl, sql, context);
            } finally {
                postExecute(context, DXLFunction.UNSPECIFIED_DDL_WRITE);
            }
        }
        return statementComplete(ddl);
    }

    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }
}
