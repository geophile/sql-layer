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

import com.foundationdb.sql.server.ServerValueEncoder;

import java.util.List;
import java.io.IOException;

public abstract class PostgresOutputter<T>
{
    protected PostgresMessenger messenger;
    protected PostgresQueryContext context;
    protected PostgresDMLStatement statement;
    protected List<PostgresType> columnTypes;
    protected int ncols;
    protected ServerValueEncoder encoder;

    public PostgresOutputter(PostgresQueryContext context,
                             PostgresDMLStatement statement) {
        this.context = context;
        this.statement = statement;
        PostgresServerSession server = context.getServer();
        messenger = server.getMessenger();
        columnTypes = statement.getColumnTypes();
        if (columnTypes != null)
            ncols = columnTypes.size();
        encoder = server.getValueEncoder();
    }

    public void beforeData() throws IOException {}

    public void afterData() throws IOException {}

    public abstract void output(T row) throws IOException;
}
