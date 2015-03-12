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
import com.foundationdb.server.service.monitor.PreparedStatementMonitor;

public class PostgresPreparedStatement implements PreparedStatementMonitor
{
    private PostgresServerSession session;
    private String name;
    private String sql;
    private PostgresStatement statement;
    private long prepareTime;

    public PostgresPreparedStatement(PostgresServerSession session, String name,
                                     String sql, PostgresStatement statement,
                                     long prepareTime) {
        this.session = session;
        this.name = name;
        this.sql = sql;
        this.statement = statement;
        this.prepareTime = prepareTime;
    }

    @Override
    public int getSessionId() {
        return session.getSessionMonitor().getSessionId();
    }

    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public String getSQL() {
        return sql;
    }
    
    @Override
    public long getPrepareTimeMillis() {
        return prepareTime;
    }

    @Override
    public int getEstimatedRowCount() {
        CostEstimate costEstimate = statement.getCostEstimate();
        if (costEstimate == null)
            return -1;
        else
            return (int)costEstimate.getRowCount();
    }

    public PostgresStatement getStatement() {
        return statement;
    }

}
