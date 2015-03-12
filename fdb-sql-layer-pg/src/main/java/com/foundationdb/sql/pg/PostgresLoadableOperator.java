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

import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerCallInvocation;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.loadableplan.LoadableOperator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import java.io.IOException;
import java.util.List;

public class PostgresLoadableOperator extends PostgresOperatorStatement
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadableOperator: execute shared");

    private ServerCallInvocation invocation;

    protected PostgresLoadableOperator(LoadableOperator loadableOperator, 
                                       ServerCallInvocation invocation,
                                       List<String> columnNames, List<PostgresType> columnTypes, List<Column> aisColumns,
                                       PostgresType[] parameterTypes)
    {
        super(null);
        super.init(loadableOperator.plan(), null, columnNames, columnTypes, aisColumns, parameterTypes, null);
        this.invocation = invocation;
    }
    
    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        bindings = PostgresLoadablePlan.setParameters(bindings, invocation);
        ServerCallContextStack stack = ServerCallContextStack.get();
        boolean success = false;
        stack.push(context, invocation);
        try {
            PostgresStatementResult result = super.execute(context, bindings, maxrows);
            success = true;
            return result;
        }
        finally {
            stack.pop(context, invocation, success);
        }
    }

}
