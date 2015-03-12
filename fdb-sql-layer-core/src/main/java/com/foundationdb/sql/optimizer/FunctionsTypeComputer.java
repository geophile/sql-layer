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

import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.sql.parser.*;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.DataTypeDescriptor;

import com.foundationdb.ais.model.Routine;

import com.foundationdb.sql.optimizer.plan.AggregateFunctionExpression;

/** Marks aggregate functions as such. */
public class FunctionsTypeComputer extends AISTypeComputer
{
    private TypesRegistryService functionsRegistry;

    public FunctionsTypeComputer(TypesRegistryService functionsRegistry) {
        this.functionsRegistry = functionsRegistry;
    }

    @Override
    protected DataTypeDescriptor computeType(ValueNode node) throws StandardException {
        switch (node.getNodeType()) {
        case NodeTypes.JAVA_TO_SQL_VALUE_NODE:
            return javaValueNode(((JavaToSQLValueNode)node).getJavaValueNode());
        default:
            return super.computeType(node);
        }
    }

    protected DataTypeDescriptor javaValueNode(JavaValueNode javaValue)
            throws StandardException {
        if (javaValue instanceof MethodCallNode) {
            return methodCallNode((MethodCallNode)javaValue);
        }
        else {
            return null;
        }
    }

    protected DataTypeDescriptor methodCallNode(MethodCallNode methodCall)
            throws StandardException {
        if (methodCall.getUserData() != null) {
            Routine routine = (Routine)methodCall.getUserData();
            return routine.getReturnValue().getType().dataTypeDescriptor();
        }
        if (methodCall.getMethodParameters().length == 1) {
            return oneArgMethodCall(methodCall);
        }
        else {
            return null;
        }
    }

    protected DataTypeDescriptor oneArgMethodCall(MethodCallNode methodCall)
            throws StandardException {
        TypesRegistryService.FunctionKind functionKind =
            functionsRegistry.getFunctionKind(methodCall.getMethodName());
        if (functionKind == TypesRegistryService.FunctionKind.AGGREGATE) {
            // Mark the method call as really an aggregate function.
            // Could do the substitution now, but that would require throwing
            // a subclass of StandardException up to visit() or something other
            // complicated control flow.
            methodCall.setJavaClassName(AggregateFunctionExpression.class.getName());
            JavaValueNode arg = methodCall.getMethodParameters()[0];
            if (arg instanceof SQLToJavaValueNode) {
                SQLToJavaValueNode jarg = (SQLToJavaValueNode)arg;
                ValueNode sqlArg = jarg.getSQLValueNode();
                return sqlArg.getType();
            }
        }
        return null;
    }

    /** Return the name of a built-in special function. */
    public static String specialFunctionName(SpecialFunctionNode node) {
        switch (node.getNodeType()) {
        case NodeTypes.USER_NODE:
        case NodeTypes.CURRENT_USER_NODE:
            return "current_user";
        case NodeTypes.SESSION_USER_NODE:
            return "session_user";
        case NodeTypes.SYSTEM_USER_NODE:
            return "system_user";
        case NodeTypes.CURRENT_SCHEMA_NODE:
            return "current_schema";
        case NodeTypes.CURRENT_ISOLATION_NODE:
        case NodeTypes.IDENTITY_VAL_NODE:
        case NodeTypes.CURRENT_ROLE_NODE:
        default:
            return null;
        }
    }

    /** Return the name of a built-in special function. */
    public static String currentDatetimeFunctionName(CurrentDatetimeOperatorNode node) {
        switch (node.getField()) {
        case DATE:
            return "current_date";
        case TIME:
            return "current_time";
        case TIMESTAMP:
            return "current_timestamp";
        default:
            return null;
        }
    }

}
