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

import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.sql.server.ServerValueEncoder;

import static com.foundationdb.sql.pg.PostgresStatement.PostgresStatementResult;

import java.util.List;
import java.io.IOException;

/** A Postgres server session. */
public interface PostgresServerSession extends ServerSession
{
    /** Return the protocol version in use. */
    public int getVersion();

    /** Return the messenger used to communicate with client. */
    public PostgresMessenger getMessenger();

    /** Return an encoder of values as bytes / strings. */
    public ServerValueEncoder getValueEncoder();

    public enum OutputFormat { TABLE, JSON, JSON_WITH_META_DATA };

    /** Get the output format. */
    public OutputFormat getOutputFormat();

    /** Prepare a statement and store by name. */
    public void prepareStatement(String name, 
                                 String sql, StatementNode stmt,
                                 List<ParameterNode> params, int[] paramTypes);

    /** Execute prepared statement. */
    public PostgresStatementResult executePreparedStatement(PostgresExecuteStatement estmt, int maxrows)
            throws IOException;

    /** Remove prepared statement with given name. */
    public void deallocatePreparedStatement(String name);

    /** Declare a named cursor. */
    public void declareStatement(String name, 
                                 String sql, StatementNode stmt);

    /** Fetch from named cursor. */
    public PostgresStatementResult fetchStatement(String name, int count) throws IOException;

    /** Remove declared cursor with given name. */
    public void closeBoundPortal(String name);
    
    public int getStatementCacheCapacity();

}
