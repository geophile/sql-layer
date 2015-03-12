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

import com.foundationdb.sql.embedded.JDBCParameterMetaData.ParameterType;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.parser.CallStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StaticMethodCallNode;
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.Arrays;
import java.util.List;

abstract class ExecutableCallStatement extends ExecutableStatement
{
    protected ServerCallInvocation invocation;
    protected JDBCParameterMetaData parameterMetaData;

    protected ExecutableCallStatement(ServerCallInvocation invocation,
                                      JDBCParameterMetaData parameterMetaData) {
        this.invocation = invocation;
        this.parameterMetaData = parameterMetaData;
    }

    public static ExecutableStatement executableStatement(CallStatementNode call,
                                                          List<ParameterNode> sqlParams,
                                                          EmbeddedQueryContext context) {
        StaticMethodCallNode methodCall = (StaticMethodCallNode)call.methodCall().getJavaValueNode();
        ServerCallInvocation invocation =
            ServerCallInvocation.of(context.getServer(), methodCall);
        return executableStatement(invocation, call, sqlParams, context);
    }

    public static ExecutableStatement executableStatement(TableName routineName,
                                                          EmbeddedQueryContext context) {
        ServerCallInvocation invocation =
            ServerCallInvocation.of(context.getServer(), routineName);
        return executableStatement(invocation, null, null, context);
    }

    protected static ExecutableStatement executableStatement(ServerCallInvocation invocation,
                                                             CallStatementNode call,
                                                             List<ParameterNode> sqlParams,
                                                             EmbeddedQueryContext context) {
        int nparams = (sqlParams == null) ? invocation.size() : sqlParams.size();
        JDBCParameterMetaData parameterMetaData = parameterMetaData(invocation, nparams, context);
        switch (invocation.getCallingConvention()) {
        case LOADABLE_PLAN:
            return ExecutableLoadableOperator.executableStatement(invocation, parameterMetaData, call, context);
        case JAVA:
            return ExecutableJavaMethod.executableStatement(invocation, parameterMetaData, context);
        case SCRIPT_FUNCTION_JAVA:
        case SCRIPT_FUNCTION_JSON:
            return ExecutableScriptFunctionJavaRoutine.executableStatement(invocation, parameterMetaData, context);
        case SCRIPT_BINDINGS:
        case SCRIPT_BINDINGS_JSON:
            return ExecutableScriptBindingsRoutine.executableStatement(invocation, parameterMetaData, context);
        default:
            throw new UnsupportedSQLException("Unknown routine", call);
        }
    }

    protected static JDBCParameterMetaData parameterMetaData(ServerCallInvocation invocation,
                                                             int nparams,
                                                             EmbeddedQueryContext context) {
        ParameterType[] ptypes = new ParameterType[nparams];
        for (int i = 0; i < nparams; i++) {
            int usage = invocation.parameterUsage(i);
            if (usage < 0) continue;
            Parameter parameter = invocation.getRoutineParameter(usage);
            TInstance type = parameter.getType();
            int jdbcType = type.typeClass().jdbcType();
            DataTypeDescriptor sqlType = type.dataTypeDescriptor();
            ptypes[i] = new ParameterType(parameter, sqlType, jdbcType, type);
        }
        return new JDBCParameterMetaData(context.getTypesTranslator(), Arrays.asList(ptypes));
    }

    public ServerCallInvocation getInvocation() {
        return invocation;
    }

    @Override
    public JDBCParameterMetaData getParameterMetaData() {
        return parameterMetaData;
    }
    
    @Override
    public StatementTypes getStatementType() {
        return StatementTypes.CALL_STMT;
    }
}
