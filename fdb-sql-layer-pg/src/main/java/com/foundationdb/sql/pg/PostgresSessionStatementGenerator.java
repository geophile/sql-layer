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

import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.TransactionControlNode;

import java.util.List;

/** SQL statements that affect session / environment state. */
public class PostgresSessionStatementGenerator extends PostgresBaseStatementGenerator
{
    public PostgresSessionStatementGenerator(PostgresServerSession server) {
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes)  {
        switch (stmt.getNodeType()) {
        case NodeTypes.SET_SCHEMA_NODE:
            return PostgresSessionStatement.Operation.USE.getStatement(stmt);
        case NodeTypes.TRANSACTION_CONTROL_NODE:
            {
                PostgresSessionStatement.Operation operation;
                switch (((TransactionControlNode)stmt).getOperation()) {
                case BEGIN:
                    operation = PostgresSessionStatement.Operation.BEGIN_TRANSACTION;
                    break;
                case COMMIT:
                    operation = PostgresSessionStatement.Operation.COMMIT_TRANSACTION;
                    break;
                case ROLLBACK:
                    operation = PostgresSessionStatement.Operation.ROLLBACK_TRANSACTION;
                    break;
                default:
                    assert false : "Unknown operation " + stmt;
                    operation = null;
                }
                return new PostgresSessionStatement(operation, stmt);
            }
        case NodeTypes.SET_TRANSACTION_ISOLATION_NODE:
            return PostgresSessionStatement.Operation.TRANSACTION_ISOLATION.getStatement(stmt);
        case NodeTypes.SET_TRANSACTION_ACCESS_NODE:
            return PostgresSessionStatement.Operation.TRANSACTION_ACCESS.getStatement(stmt);
        case NodeTypes.SET_CONFIGURATION_NODE:
            return PostgresSessionStatement.Operation.SET_CONFIGURATION.getStatement(stmt);
        case NodeTypes.SHOW_CONFIGURATION_NODE:
            return PostgresSessionStatement.Operation.SHOW_CONFIGURATION.getStatement(stmt);
        case NodeTypes.SET_CONSTRAINTS_NODE:
            return PostgresSessionStatement.Operation.SET_CONSTRAINTS.getStatement(stmt);
        default:
            return null;
        }
    }
}
