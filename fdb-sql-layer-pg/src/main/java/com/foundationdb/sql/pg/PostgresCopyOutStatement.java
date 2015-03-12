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

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.CopyStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.externaldata.CsvFormat;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;

import static com.foundationdb.sql.pg.PostgresCopyInStatement.csvFormat;
import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.io.*;
import java.util.*;

/** COPY ... TO */
public class PostgresCopyOutStatement extends PostgresOperatorStatement
{
    private File toFile;
    private CsvFormat csvFormat;

    public PostgresCopyOutStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        CopyStatementNode copyStmt = (CopyStatementNode)stmt;
        try {
            stmt = copyStmt.asQuery();
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }        
        PostgresStatement pstmt = super.finishGenerating(server, sql, stmt,
                                                         params, paramTypes);
        assert (pstmt == this);
        if (copyStmt.getFilename() != null)
            toFile = new File(copyStmt.getFilename());
        CopyStatementNode.Format format = copyStmt.getFormat();
        if (format == null)
            format = CopyStatementNode.Format.CSV;
        switch (format) {
        case CSV:
            csvFormat = csvFormat(copyStmt, server);
            if (copyStmt.isHeader()) {
                csvFormat.setHeadings(getColumnNames());
            }
            break;
        default:
            throw new UnsupportedSQLException("COPY FORMAT " + format);
        }
        return this;
    }

    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        if (toFile == null)
            return super.execute(context, bindings, maxrows);

        PostgresServerSession server = context.getServer();
        server.getSessionMonitor().countEvent(StatementTypes.OTHER_STMT);
        int nrows = 0;
        Cursor cursor = null;
        OutputStream outputStream = null;
        try {
            preExecute(context, DXLFunction.UNSPECIFIED_DML_READ);
            cursor = context.startCursor(this, bindings);
            outputStream = new FileOutputStream(toFile);
            int ncols = getColumnTypes().size();
            PostgresCopyCsvOutputter outputter = 
                new PostgresCopyCsvOutputter(context, this, csvFormat);
            if (csvFormat.getHeadings() != null) {
                outputter.outputHeadings(outputStream);
                nrows++;
            }
            Row row;
            while ((row = cursor.next()) != null) {
                outputter.output(row, outputStream);
                nrows++;
            }
        }
        finally {
            if (outputStream != null)
                outputStream.close();
            context.finishCursor(this, cursor, nrows, false);
            postExecute(context, DXLFunction.UNSPECIFIED_DML_READ);
        }
        return commandComplete("COPY " + nrows);
    }

    @Override
    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresCopyCsvOutputter(context, this, csvFormat);
    }
    
    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
    }

    @Override
    public boolean putInCache() {
        return false;
    }

}
