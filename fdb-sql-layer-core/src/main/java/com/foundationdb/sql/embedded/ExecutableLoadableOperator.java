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
package com.foundationdb.sql.embedded;

import com.foundationdb.sql.embedded.JDBCResultSetMetaData.ResultColumn;

import com.foundationdb.qp.loadableplan.LoadableOperator;
import com.foundationdb.qp.loadableplan.LoadablePlan;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.SparseArrayQueryBindings;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.parser.CallStatementNode;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.ArrayList;
import java.util.List;

class ExecutableLoadableOperator extends ExecutableQueryOperatorStatement
{
    private ServerCallInvocation invocation;

    public static ExecutableStatement executableStatement(ServerCallInvocation invocation,
                                                          JDBCParameterMetaData parameterMetaData,
                                                          CallStatementNode call,
                                                          EmbeddedQueryContext context) {
        JDBCConnection conn = context.getServer();
        LoadablePlan<?> plan = 
            conn.getRoutineLoader().loadLoadablePlan(conn.getSession(),
                                                     invocation.getRoutineName());
        long aisGeneration = context.getAIS().getGeneration();
        JDBCResultSetMetaData resultSetMetaData = resultSetMetaData(plan, context);
        if (plan instanceof LoadableOperator)
            return new ExecutableLoadableOperator((LoadableOperator)plan,
                                                  invocation, aisGeneration,
                                                  resultSetMetaData, parameterMetaData);
        throw new UnsupportedSQLException("Unsupported loadable plan", call);
    }

    protected ExecutableLoadableOperator(LoadableOperator loadableOperator, 
                                         ServerCallInvocation invocation,
                                         long aisGeneration,
                                         JDBCResultSetMetaData resultSetMetaData,
                                         JDBCParameterMetaData parameterMetaData) {
        super(loadableOperator.plan(), aisGeneration, resultSetMetaData, parameterMetaData, null);
        this.invocation = invocation;
    }
    
    @Override
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
        bindings = setParameters(bindings, invocation);
        ServerCallContextStack stack = ServerCallContextStack.get();
        boolean success = false;
        stack.push(context, invocation);
        try {
            ExecuteResults results = super.execute(context, bindings);
            success = true;
            return results;
        }
        finally {
            stack.pop(context, invocation, success);
        }
    }

    protected static JDBCResultSetMetaData resultSetMetaData(LoadablePlan<?> plan,
                                                             EmbeddedQueryContext context) {
        List<String> columnNames = plan.columnNames();
        int[] jdbcTypes = plan.jdbcTypes();
        List<ResultColumn> columns = new ArrayList<>(jdbcTypes.length);
        for (int i = 0; i < jdbcTypes.length; i++) {
            String name = columnNames.get(i);
            int jdbcType = jdbcTypes[i];
            DataTypeDescriptor sqlType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType);
            TInstance type = context.getTypesTranslator().typeForSQLType(sqlType);
            ResultColumn column = new ResultColumn(name, jdbcType, sqlType,
                                                   null, type, null);
            columns.add(column);
        }
        return new JDBCResultSetMetaData(context.getTypesTranslator(), columns);
    }

    protected static QueryBindings setParameters(QueryBindings bindings, ServerCallInvocation invocation) {
        if (!invocation.parametersInOrder()) {
            if (invocation.hasParameters()) {
                QueryBindings calleeBindings = new SparseArrayQueryBindings();
                invocation.copyParameters(bindings, calleeBindings);
                bindings = calleeBindings;
            }
            else {
                invocation.copyParameters(null, bindings);
            }
        }
        return bindings;
    }

}
