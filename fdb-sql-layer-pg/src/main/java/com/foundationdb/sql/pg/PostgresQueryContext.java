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

import com.foundationdb.sql.server.ServerQueryContext;

import com.foundationdb.qp.operator.CursorBase;
import com.foundationdb.qp.operator.QueryBindings;

public class PostgresQueryContext extends ServerQueryContext<PostgresServerSession>
{
    public PostgresQueryContext(PostgresServerSession server) {
        super(server);
    }

    public boolean isColumnBinary(int i) {
        return false;
    }

    /**
     * Returns an open cursor.
     * finishCursor must be called with the returned cursor in a finally block.
     * Do not call close() on the returned cursor, call finishCursor.
     * If finish cursor was previously called with suspended=true, that cursor may be returned here.
     */
    public <T extends CursorBase> T startCursor(PostgresCursorGenerator<T> generator, QueryBindings bindings) {
        return generator.openCursor(this, bindings);
    }

    /**
     *
     * @param cursor a cursor as returned by startCursor
     * @param nrows the number of rows processed since the last call to startCursor
     * @param suspended if true the cursor is just suspended, call startCursor again to resume processing
     */
    public <T extends CursorBase> boolean finishCursor(PostgresCursorGenerator<T> generator, T cursor, int nrows, boolean suspended) {
        generator.closeCursor(cursor);
        return false;
    }

}
