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

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.routines.ScriptEvaluator;
import com.foundationdb.server.service.routines.ScriptPool;
import com.foundationdb.sql.script.ScriptBindingsRoutine;
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.server.ServerJavaRoutine;

import java.util.List;

public class PostgresScriptBindingsRoutine extends PostgresJavaRoutine
{
    private ScriptPool<ScriptEvaluator> pool;

    public static PostgresScriptBindingsRoutine statement(PostgresServerSession server, 
                                                          ServerCallInvocation invocation,
                                                          List<String> columnNames, 
                                                          List<PostgresType> columnTypes,
                                                          List<Column> aisColumns,
                                                          PostgresType[] parameterTypes) {
        long[] aisGeneration = new long[1];
        ScriptPool<ScriptEvaluator> pool = server.getRoutineLoader()
            .getScriptEvaluator(server.getSession(), invocation.getRoutineName(),
                                aisGeneration);
        return new PostgresScriptBindingsRoutine(pool, invocation,
                                                 columnNames, columnTypes, aisColumns,
                                                 parameterTypes);
    }

    protected PostgresScriptBindingsRoutine(ScriptPool<ScriptEvaluator> pool,
                                            ServerCallInvocation invocation,
                                            List<String> columnNames, 
                                            List<PostgresType> columnTypes,
                                            List<Column> aisColumns,
                                            PostgresType[] parameterTypes) {
        super(invocation, columnNames, columnTypes, aisColumns, parameterTypes);
        this.pool = pool;
    }

    @Override
    protected ServerJavaRoutine javaRoutine(PostgresQueryContext context, QueryBindings bindings) {
        return new ScriptBindingsRoutine(context, bindings, invocation, pool);
    }
    
}
