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

import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.ExecuteStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.ValueNode;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

public class PostgresExecuteStatement extends PostgresBaseCursorStatement
{
    private String name;
    private List<TPreptimeValue> paramValues; 

    public String getName() {
        return name;
    }

    public void setParameters(QueryBindings bindings) {
        for (int i = 0; i < paramValues.size(); i++) {
            bindings.setValue(i, paramValues.get(i).value());
        }
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        ExecuteStatementNode execute = (ExecuteStatementNode)stmt;
        this.name = execute.getName();
        paramValues = new ArrayList<>();
        for (ValueNode param : execute.getParameterList()) {
            TInstance type;
            if (!(param instanceof ConstantNode)) {
                throw new UnsupportedSQLException("EXECUTE arguments must be constants", param);
            }
            if (param.getType() != null)
                type = server.typesTranslator().typeForSQLType(param.getType());
            else
                type = server.typesTranslator().typeForString();
            ConstantNode constant = (ConstantNode)param;
            Object value = constant.getValue();
            paramValues.add(ValueSources.fromObject(value, type));
        }
        return this;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        // Execute will do it.
    }

    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        return server.executePreparedStatement(this, maxrows);
    }
    
    @Override
    public boolean putInCache() {
        return true;
    }

}
