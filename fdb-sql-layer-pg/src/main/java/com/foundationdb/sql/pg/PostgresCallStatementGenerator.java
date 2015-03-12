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

import com.foundationdb.sql.server.ServerCallInvocation;

import com.foundationdb.sql.parser.CallStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.StaticMethodCallNode;

import com.foundationdb.server.error.CantCallScriptLibraryException;
import com.foundationdb.server.explain.Explainable;

import java.util.List;

/**
 * SQL statements that affect session / environment state.
 */
public class PostgresCallStatementGenerator extends PostgresBaseStatementGenerator
{
    public PostgresCallStatementGenerator(PostgresServerSession server)
    {
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes)
    {
        if (!(stmt instanceof CallStatementNode))
            return null;
        CallStatementNode call = (CallStatementNode)stmt;
        StaticMethodCallNode methodCall = (StaticMethodCallNode)call.methodCall().getJavaValueNode();
        // This will signal error if undefined, so any special handling of
        // non-AIS CALL statements needs to be tested by an earlier generator.
        ServerCallInvocation invocation = ServerCallInvocation.of(server, methodCall);
        final PostgresStatement pstmt;
        switch (invocation.getCallingConvention()) {
        case LOADABLE_PLAN:
            pstmt = PostgresLoadablePlan.statement(server, invocation);
            break;
        case SCRIPT_LIBRARY:
            throw new CantCallScriptLibraryException(stmt);
        default:
            pstmt = PostgresJavaRoutine.statement(server, invocation,
                                                  params, paramTypes);
        }
        // The above makes extensive use of the AIS. This doesn't fit well into the
        // create and then init, so just mark with AIS now.
        if (pstmt != null)
            pstmt.setAISGeneration(server.getAIS().getGeneration());
        return pstmt;
    }

    public static Explainable explainable(PostgresServerSession server,
                                          CallStatementNode call, 
                                          List<ParameterNode> params, int[] paramTypes) {
        StaticMethodCallNode methodCall = (StaticMethodCallNode)call.methodCall().getJavaValueNode();
        ServerCallInvocation invocation = ServerCallInvocation.of(server, methodCall);
        switch (invocation.getCallingConvention()) {
        case LOADABLE_PLAN:
            return PostgresLoadablePlan.explainable(server, invocation);
        default:
            return PostgresJavaRoutine.explainable(server, invocation,
                                                   params, paramTypes);
        }
    }
}
