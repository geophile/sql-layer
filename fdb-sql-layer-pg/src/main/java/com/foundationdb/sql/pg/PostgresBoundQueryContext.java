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

import com.foundationdb.qp.operator.CursorBase;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.monitor.CursorMonitor;

public class PostgresBoundQueryContext extends PostgresQueryContext 
                                       implements CursorMonitor
{
    private QueryBindings bindings;
    private static enum State { NORMAL, UNOPENED, SUSPENDED, EXHAUSTED };
    private boolean reportSuspended;
    private State state;
    private PostgresPreparedStatement statement;
    private boolean[] columnBinary;
    private boolean defaultColumnBinary;
    private CursorBase<?> cursor;
    private String portalName;
    private long creationTime;
    private volatile int nrows;
    
    public PostgresBoundQueryContext(PostgresServerSession server,
                                     PostgresPreparedStatement statement,
                                     String portalName,
                                     boolean canSuspend, boolean reportSuspended) {
        super(server);
        this.statement = statement;
        this.portalName = portalName;
        this.state = canSuspend ? State.UNOPENED : State.NORMAL;
        this.reportSuspended = reportSuspended;
        this.creationTime = System.currentTimeMillis();
    }

    public QueryBindings getBindings() {
        return bindings;
    }

    protected void setBindings(QueryBindings bindings) {
        this.bindings = bindings;
    }

    public PostgresPreparedStatement getStatement() {
        return statement;
    }
    
    protected void setColumnBinary(boolean[] columnBinary, boolean defaultColumnBinary) {
        this.columnBinary = columnBinary;
        this.defaultColumnBinary = defaultColumnBinary;
    }

    @Override
    public boolean isColumnBinary(int i) {
        if ((columnBinary != null) && (i < columnBinary.length))
            return columnBinary[i];
        else
            return defaultColumnBinary;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends CursorBase> T startCursor(PostgresCursorGenerator<T> generator, QueryBindings bindings) {
        switch (state) {
        case NORMAL:
        case UNOPENED:
        default:
            return super.startCursor(generator, bindings);
        case SUSPENDED:
            return (T)cursor;
        case EXHAUSTED:
            return null;
        }
    }

    @Override
    public <T extends CursorBase> boolean finishCursor(PostgresCursorGenerator<T> generator, T cursor, int nrows, boolean suspended) {
        this.nrows += nrows;
        if (suspended && (state != State.NORMAL)) {
            this.state = State.SUSPENDED;
            this.cursor = cursor;
            return reportSuspended;
        }
        this.state = State.EXHAUSTED;
        this.cursor = null;
        return super.finishCursor(generator, cursor, nrows, suspended);
    }

    /**
     * Closes this context, and any suspended cursors. You should call finishCursor on any cursors before calling this.
     */
    protected void close() {
        if (cursor != null) {
            cursor.close();
            cursor = null;
            state = State.EXHAUSTED;
        }        
    }

    /* CursorMonitor */

    @Override
    public int getSessionId() {
        return getServer().getSessionMonitor().getSessionId();
    }

    @Override
    public String getName() {
        return portalName;
    }

    @Override
    public String getSQL() {
        return statement.getSQL();
    }

    @Override
    public String getPreparedStatementName() {
        return statement.getName();
    }

    @Override
    public long getCreationTimeMillis() {
        return creationTime;
    }

    @Override
    public int getRowCount() {
        return nrows;
    }

}
