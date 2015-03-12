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

import com.foundationdb.sql.parser.StatementNode;

import static com.foundationdb.sql.pg.PostgresStatement.PostgresStatementResult;

import java.io.IOException;

public class PostgresStatementResults
{
    public static PostgresStatementResult commandComplete(final String tag,
                                                          final int nrows) {
        return new PostgresStatementResult() {
                @Override
                public void sendCommandComplete(PostgresMessenger messenger) throws IOException {
                    messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
                    messenger.writeString(tag);
                    messenger.sendMessage();
                }
                
                public int getRowsProcessed() {
                    return nrows;
                }
            };
    }

    public static PostgresStatementResult commandComplete(String tag) {
        return commandComplete(tag, 0);
    }

    public static PostgresStatementResult statementComplete(StatementNode stmt, int nrows) {
        return commandComplete(stmt.statementToString(), nrows);
    }

    public static PostgresStatementResult statementComplete(StatementNode stmt) {
        return statementComplete(stmt, 0);
    }

    public static PostgresStatementResult portalSuspended(final int nrows) {
        return new PostgresStatementResult() {
                @Override
                public void sendCommandComplete(PostgresMessenger messenger) throws IOException {
                    messenger.beginMessage(PostgresMessages.PORTAL_SUSPENDED_TYPE.code());
                    messenger.sendMessage();
                }
                
                public int getRowsProcessed() {
                    return nrows;
                }
            };
    }

    // Used for EXECUTE and other recursive statements that have already sent result.
    public static PostgresStatementResult noResult(final int nrows) {
        return new PostgresStatementResult() {
                @Override
                public void sendCommandComplete(PostgresMessenger messenger) {
                }
                
                public int getRowsProcessed() {
                    return nrows;
                }
            };
    }

    // Used when the connection has been shut down.
    public static PostgresStatementResult noResult() {
        return noResult(0);
    }
}
