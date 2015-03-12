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

import com.foundationdb.sql.parser.PrepareStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.optimizer.ParameterFinder;

import com.foundationdb.qp.operator.QueryBindings;

import java.util.List;
import java.io.IOException;

public class PostgresPrepareStatement extends PostgresBaseCursorStatement
{
    private String name;
    private String sql;
    private StatementNode stmt;
    private List<ParameterNode> params;
    private int[] paramTypes;

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        PrepareStatementNode prepare = (PrepareStatementNode)stmt;
        this.name = prepare.getName();
        this.stmt = prepare.getStatement();
        if (params == null)
            params = new ParameterFinder().find(this.stmt);
        this.params = params;
        this.paramTypes = paramTypes;
        this.sql = sql.substring(this.stmt.getBeginOffset(), this.stmt.getEndOffset() + 1);
        return this;
    }
    
    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.prepareStatement(name, sql, stmt, params, paramTypes);
        return commandComplete("PREPARE");
    }
    
    @Override
    public boolean putInCache() {
        return false;
    }

}
