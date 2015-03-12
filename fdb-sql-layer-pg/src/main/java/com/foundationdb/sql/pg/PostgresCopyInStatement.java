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

import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.CopyStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.externaldata.CsvFormat;
import com.foundationdb.server.service.externaldata.ExternalDataService;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.sql.server.ServerTransaction;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/** COPY ... FROM */
public class PostgresCopyInStatement extends PostgresBaseStatement
{
    private Table toTable;
    private List<Column> toColumns;
    private File fromFile;
    private CopyStatementNode.Format format;
    private String encoding;
    private CsvFormat csvFormat;
    private long skipRows;
    private long commitFrequency;
    private int maxRetries;

    private static final Logger logger = LoggerFactory.getLogger(PostgresCopyInStatement.class);
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresCopyInStatement: execute shared");

    protected PostgresCopyInStatement() {
    }
    

    public static CsvFormat csvFormat(CopyStatementNode copyStmt, 
                                      PostgresServerSession server) {
        String encoding = copyStmt.getEncoding();
        if (encoding == null)
            encoding = server.getMessenger().getEncoding();
        CsvFormat format = new CsvFormat(encoding);
        if (copyStmt.getDelimiter() != null)
            format.setDelimiter(copyStmt.getDelimiter());
        if (copyStmt.getQuote() != null)
            format.setQuote(copyStmt.getQuote());
        if (copyStmt.getEscape() != null)
            format.setEscape(copyStmt.getEscape());
        if (copyStmt.getNullString() != null)
            format.setNullString(copyStmt.getNullString());
        return format;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        CopyStatementNode copyStmt = (CopyStatementNode)stmt;
        String schemaName = copyStmt.getTableName().getSchemaName();
        String tableName = copyStmt.getTableName().getTableName();
        if (schemaName == null)
            schemaName = server.getDefaultSchemaName();
        toTable = server.getAIS().getTable(schemaName, tableName);
        if (toTable == null)
            throw new NoSuchTableException(schemaName, tableName, 
                                           copyStmt.getTableName());
        if (copyStmt.getColumnList() == null)
            toColumns = toTable.getColumns();
        else {
            toColumns = new ArrayList<>(copyStmt.getColumnList().size());
            for (ResultColumn rc : copyStmt.getColumnList()) {
                ColumnReference cref = rc.getReference();
                Column column = toTable.getColumn(cref.getColumnName());
                if (column == null)
                    throw new NoSuchColumnException(cref.getColumnName(), cref);
                toColumns.add(column);
            }
        }
        if (copyStmt.getFilename() != null)
            fromFile = new File(copyStmt.getFilename());
        format = copyStmt.getFormat();
        if (format == null)
            format = CopyStatementNode.Format.CSV;
        switch (format) {
        case CSV:
            csvFormat = csvFormat(copyStmt, server);
            if (copyStmt.isHeader()) {
                skipRows = 1;
            }
            break;
        case MYSQL_DUMP:
            encoding = copyStmt.getEncoding();
            if (encoding == null)
                encoding = server.getMessenger().getEncoding();
            break;
        default:
            throw new UnsupportedSQLException("COPY FORMAT " + format);
        }
        commitFrequency = copyStmt.getCommitFrequency();
        if (commitFrequency == 0) {
            commitFrequency = server.getTransactionPeriodicallyCommit() == ServerTransaction.PeriodicallyCommit.OFF ?
                    ExternalDataService.COMMIT_FREQUENCY_NEVER :
                    ExternalDataService.COMMIT_FREQUENCY_PERIODICALLY;
        }
        maxRetries = copyStmt.getMaxRetries();
        return this;
    }

    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.getSessionMonitor().countEvent(StatementTypes.OTHER_STMT);
        Session session = server.getSession();
        ExternalDataService externalData = server.getExternalDataService();
        InputStream istr;
        long nrows = 0;
        if (fromFile != null)
            istr = new FileInputStream(fromFile);
        else
            // Always use a stream: we align records and messages, but
            // this is not a requirement on the client.
            istr = new PostgresCopyInputStream(server.getMessenger(), 
                                               toColumns.size());
        try {
            preExecute(context, DXLFunction.UNSPECIFIED_DML_WRITE);
            switch (format) {
            case CSV:
                nrows = externalData.loadTableFromCsv(session, istr, csvFormat, skipRows,
                                                      toTable, toColumns,
                                                      commitFrequency, maxRetries,
                                                      context);
                break;
            case MYSQL_DUMP:
                nrows = externalData.loadTableFromMysqlDump(session, istr, encoding,
                                                            toTable, toColumns,
                                                            commitFrequency, maxRetries,
                                                            context);
                break;
            }
        }
        finally {
            postExecute(context, DXLFunction.UNSPECIFIED_DML_WRITE);
            istr.close();
        }
        return commandComplete("COPY " + nrows);
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.NONE;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public boolean putInCache() {
        return false;
    }

    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

}
