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

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.server.error.UnableToExplainException;
import com.foundationdb.server.error.UnsupportedExplainException;
import com.foundationdb.sql.optimizer.OperatorCompiler;
import com.foundationdb.sql.optimizer.plan.BasePlannable;
import com.foundationdb.sql.optimizer.rule.ExplainPlanContext;

import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.ExplainStatementNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.ParameterNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** SQL statement to explain another one. */
public class PostgresExplainStatementGenerator extends PostgresBaseStatementGenerator
{
    private OperatorCompiler compiler;

    public PostgresExplainStatementGenerator(PostgresServerSession server) {
        compiler = (OperatorCompiler)server.getAttribute("compiler");
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes)  {
        if (stmt.getNodeType() != NodeTypes.EXPLAIN_STATEMENT_NODE)
            return null;
        StatementNode innerStmt = ((ExplainStatementNode)stmt).getStatement();
        if (compiler == null)
            throw new UnsupportedExplainException();
        if (!(innerStmt instanceof DMLStatementNode))
            throw new UnableToExplainException ();
        return new PostgresExplainStatement(compiler);
    }
}
