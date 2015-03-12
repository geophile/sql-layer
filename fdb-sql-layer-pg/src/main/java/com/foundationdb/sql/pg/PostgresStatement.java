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

import com.foundationdb.sql.optimizer.plan.CostEstimate;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerStatement;

import com.foundationdb.qp.operator.QueryBindings;

import java.io.IOException;
import java.util.List;

/**
 * An SQL statement compiled for use with Postgres server.
 * @see PostgresStatementGenerator
 */
public interface PostgresStatement extends ServerStatement
{
    /** Get the types of any parameters. */
    public PostgresType[] getParameterTypes();

    /** Send a description message.
     * @param always Send row description even if no rows.
     * @param params Send parameter description, too.
     */
    public void sendDescription(PostgresQueryContext context, 
                                boolean always, boolean params) throws IOException;

    public interface PostgresStatementResult {
        public void sendCommandComplete(PostgresMessenger messenger) throws IOException;
        public int getRowsProcessed();
    }

    /** Execute statement and output results. Return number of rows processed. */
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException;

    /** Whether or not the generation has been set */
    public boolean hasAISGeneration();

    /** Set generation this statement was created under */
    public void setAISGeneration(long aisGeneration);

    /** Get generation this statement was created under */
    public long getAISGeneration();

    /** Finish constructing this statement. Returning a new instance is allowed. */
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes);

    /** Should this statement be put into the statement cache? */
    public boolean putInCache();

    /** Get the estimated cost, if known and applicable. */
    public CostEstimate getCostEstimate();

}
