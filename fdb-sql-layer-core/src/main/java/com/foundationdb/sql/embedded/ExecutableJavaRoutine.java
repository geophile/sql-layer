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

import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.server.ServerJavaRoutine;

import com.foundationdb.qp.operator.QueryBindings;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Queue;

abstract class ExecutableJavaRoutine extends ExecutableCallStatement
{
    protected long aisGeneration;
    
    protected ExecutableJavaRoutine(ServerCallInvocation invocation,
                                    long aisGeneration,
                                    JDBCParameterMetaData parameterMetaData) {
        super(invocation, parameterMetaData);
        this.aisGeneration = aisGeneration;
    }

    protected abstract ServerJavaRoutine javaRoutine(EmbeddedQueryContext context, QueryBindings bindings);

    @Override
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
        Queue<ResultSet> resultSets = null;
        ServerJavaRoutine call = javaRoutine(context, bindings);
        call.push();
        boolean success = false;
        try {
            call.setInputs();
            call.invoke();
            resultSets = call.getDynamicResultSets();
            call.getOutputs();
            success = true;
        }
        finally {
            if ((resultSets != null) && !success) {
                while (!resultSets.isEmpty()) {
                    try {
                        resultSets.remove().close();
                    }
                    catch (SQLException ex) {
                    }
                }
            }
            call.pop(success);
        }
        return new ExecuteResults(resultSets);
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

    @Override
    public long getAISGeneration() {
        return aisGeneration;
    }
}
