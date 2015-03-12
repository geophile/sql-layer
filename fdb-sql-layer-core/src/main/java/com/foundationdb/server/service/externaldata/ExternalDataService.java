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
package com.foundationdb.server.service.externaldata;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.FormatOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;

public interface ExternalDataService {
    /**
     * Dump entire group, starting at the given table's depth, in JSON format.
     *
     * @param depth How far down the branch to dump: -1 = all, 0 = just tableName, 1 = children, 2 = grand-children, ...
     * @param withTransaction If <code>true</code>, start a transaction before scanning.
     */
    void dumpAllAsJson(Session session, PrintWriter writer,
                       String schemaName, String tableName,
                       int depth, boolean withTransaction, FormatOptions options);

    /**
     * Dump selected branches, identified by a list of PRIMARY KEY files, in JSON format.
     *
     * @param keys PRIMARY KEY values, which each may be multiple columns, to dump.
     * @param depth How far down the branch to dump: -1 = all, 0 = just tableName, 1 = children, 2 = grand-children, ...
     * @param withTransaction If <code>true</code>, start a transaction before scanning.
     */
    void dumpBranchAsJson(Session session, PrintWriter writer,
                          String schemaName, String tableName, 
                          List<List<Object>> keys, int depth,
                          boolean withTransaction, FormatOptions options);

    /**
     * Dump selected branches, given a generator of branch rows.
     */
    void dumpBranchAsJson(Session session, PrintWriter writer,
                          String schemaName, String tableName, 
                          Operator scan, RowType scanType, int depth,
                          boolean withTransaction, FormatOptions options);

    static final long COMMIT_FREQUENCY_NEVER = -1;
    static final long COMMIT_FREQUENCY_PERIODICALLY = -2;

    long loadTableFromCsv(Session session, InputStream inputStream, 
                          CsvFormat format, long skipRows,
                          Table toTable, List<Column> toColumns,
                          long commitFrequency, int maxRetries,
                          QueryContext context) throws IOException;

    long loadTableFromMysqlDump(Session session, InputStream inputStream, String encoding,
                                Table toTable, List<Column> toColumns,
                                long commitFrequency, int maxRetries,
                                QueryContext context) throws IOException;
    
}
