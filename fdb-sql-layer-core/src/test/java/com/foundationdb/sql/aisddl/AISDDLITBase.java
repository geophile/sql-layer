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
package com.foundationdb.sql.aisddl;

import com.foundationdb.sql.ServerSessionITBase;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.StatementNode;

public class AISDDLITBase extends ServerSessionITBase {
    protected void executeDDL(String sql) throws Exception {
        // Most of the state in this depends on the current AIS, which changes
        // as a result of this, so it's simplest to just make a new session
        // every time. Only views need all of the binder state, but
        // it's just as easy to make the parser this way.
        TestSession session = new TestSession();
        StatementNode stmt = session.getParser().parseStatement(sql);
        assert (stmt instanceof DDLStatementNode) : stmt;
        AISDDL.execute((DDLStatementNode)stmt, sql, new TestQueryContext(session));
    }

}
