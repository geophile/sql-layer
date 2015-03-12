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
package com.foundationdb.sql.server;

import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.JavaValueNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.SQLToJavaValueNode;
import com.foundationdb.sql.parser.StaticMethodCallNode;
import com.foundationdb.sql.parser.ValueNode;

import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.NoSuchRoutineException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;

import java.util.Arrays;

public class ServerCallInvocation extends ServerRoutineInvocation
{
    private final Object[] constantArgs;
    private final int[] parameterArgs;

    protected ServerCallInvocation(Routine routine,
                                   Object[] constantArgs,
                                   int[] parameterArgs) {
        super(routine);
        this.constantArgs = constantArgs;
        this.parameterArgs = parameterArgs;
    }

    public static ServerCallInvocation of(ServerSession server,
                                          StaticMethodCallNode methodCall) {
        String schemaName, routineName;
        if (methodCall.getProcedureName() == null) {
            schemaName = null;
            routineName = methodCall.getMethodName();
        }
        else {
            schemaName = methodCall.getProcedureName().getSchemaName();
            routineName = methodCall.getProcedureName().getTableName();
        }
        if (schemaName == null) {
            schemaName = server.getDefaultSchemaName();
        }
        Routine routine = server.getAIS().getRoutine(schemaName, routineName);
        if (routine == null) 
            throw new NoSuchRoutineException(schemaName, routineName);
        Object[] constantArgs = null;
        int[] parameterArgs = null;
        JavaValueNode[] margs = methodCall.getMethodParameters();
        if (margs != null) {
            constantArgs = new Object[margs.length];
            parameterArgs = new int[margs.length];
            Arrays.fill(parameterArgs, -1);
            for (int i = 0; i < margs.length; i++) {
                JavaValueNode marg = margs[i];
                if (marg instanceof SQLToJavaValueNode) {
                    ValueNode sqlArg = ((SQLToJavaValueNode)marg).getSQLValueNode();
                    if (sqlArg instanceof ConstantNode) {
                        constantArgs[i] = ((ConstantNode)sqlArg).getValue();
                        continue;
                    }
                    else if (sqlArg instanceof ParameterNode) {
                        parameterArgs[i] = ((ParameterNode)sqlArg).getParameterNumber();
                        continue;
                    }
                }
                throw new UnsupportedSQLException("CALL parameter must be constant",
                                                  marg);
            }
        }
        return new ServerCallInvocation(routine, constantArgs, parameterArgs);
    }

    public static ServerCallInvocation of(ServerSession server, TableName routineName) {
        Routine routine = server.getAIS().getRoutine(routineName);
        if (routine == null) 
            throw new NoSuchRoutineException(routineName);
        int nparams = routine.getParameters().size();
        Object[] constantArgs = new Object[nparams];
        int[] parameterArgs = new int[nparams];
        for (int i = 0; i < nparams; i++) {
            parameterArgs[i] = i;
        }
        return new ServerCallInvocation(routine, constantArgs, parameterArgs);
    }

    public int parameterUsage(int param) {
        if (parameterArgs != null) {
            for (int i = 0; i < parameterArgs.length; i++) {
                if (parameterArgs[i] == param) {
                    return i;
                }
            }
        }
        return -1;
    }

    public boolean hasParameters() {
        for (int index : parameterArgs) {
            if (index != -1) {
                return true;
            }
        }
        return false;
    }

    public boolean parametersInOrder() {
        for (int i = 0; i < parameterArgs.length; i++) {
            if (i != parameterArgs[i]) {
                return false;
            }
        }
        return true;
    }

    public void copyParameters(QueryBindings source, QueryBindings target) {
        for (int i = 0; i < parameterArgs.length; i++) {
            if (parameterArgs[i] < 0) {
                TInstance type = null;
                if (constantArgs[i] == null)
                    type = this.getType(i);
                target.setValue(i, ValueSources.valuefromObject(constantArgs[i], type));
            }
            else {
                target.setValue(i, source.getValue(parameterArgs[i]));
            }
        }
    }

    @Override
    public int size() {
        if (constantArgs == null)
            return 0;
        else
            return constantArgs.length;
    }

    public int getParameterNumber(int i) {
        return parameterArgs[i];
    }

    public Object getConstantValue(int i) {
        return constantArgs[i];
    }

    @Override
    public ServerJavaValues asValues(ServerQueryContext context, QueryBindings bindings) {
        return new Values(context, bindings);
    }

    protected class Values extends ServerJavaValues {
        private ServerQueryContext context;
        private QueryBindings bindings;

        protected Values(ServerQueryContext context, QueryBindings bindings) {
            this.context = context;
            this.bindings = bindings;
        }

        @Override
        protected int size() {
            return getRoutine().getParameters().size();
        }

        @Override
        protected ServerQueryContext getContext() {
            return context;
        }

        @Override
        protected ValueSource getValue(int index) {
            TInstance type = getType(index);
            TClass tclass = type.typeClass();
            ValueSource source;
            if (parameterArgs[index] < 0) {
                source = ValueSources.valuefromObject(constantArgs[index], type);
                if (source.getType().typeClass().equals(tclass))
                    return source; // Literal value matches.
            }
            else {
                source = bindings.getValue(parameterArgs[index]);
            }
            // Constants passed or parameters bound may not be of the
            // type specified in the signature.
            Value value = new Value(type);
            TExecutionContext executionContext = 
                new TExecutionContext(null, null, type,
                                      context, null, null, null);
            tclass.fromObject(executionContext, source, value);
            return value;
        }

        @Override
        protected TInstance getType(int index) {
            return ServerCallInvocation.this.getType(index);
        }

        @Override
        protected void setValue(int index, ValueSource source) {
            if (parameterArgs[index] < 0) {
                // An INOUT passed as a constant; do not overwrite it.
            }
            else {
                bindings.setValue(parameterArgs[index], source);
            }
        }

        @Override
        protected java.sql.ResultSet toResultSet(int index, Object resultSet) {
            throw new UnsupportedOperationException();
        }
    }

}
