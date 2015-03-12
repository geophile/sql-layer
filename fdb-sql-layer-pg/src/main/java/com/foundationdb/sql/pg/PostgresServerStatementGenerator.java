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

import java.util.List;

import com.foundationdb.sql.parser.AlterServerNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;

public class PostgresServerStatementGenerator extends
        PostgresBaseStatementGenerator {

    public PostgresServerStatementGenerator (PostgresServerSession server) {
    }
    
    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes) {

        if (stmt.getNodeType() != NodeTypes.ALTER_SERVER_NODE) 
            return null;
        return new PostgresServerStatement ((AlterServerNode)stmt);
    }
}
