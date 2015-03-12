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

import java.io.IOException;
import java.util.List;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;

/**
 * An ordinary SQL statement.
 */
public abstract class PostgresDMLStatement extends PostgresBaseStatement
{
    private RowType resultRowType;
    private List<String> columnNames;
    private List<PostgresType> columnTypes;
    private List<Column> aisColumns;
    private PostgresType[] parameterTypes;

    protected PostgresDMLStatement() {
    }

    protected void init(PostgresType[] parameterTypes) {
        this.parameterTypes = parameterTypes;
    }

    protected void init(RowType resultsRowType,
                        List<String> columnNames,
                        List<PostgresType> columnTypes,
                        List<Column> aisColumns,
                        PostgresType[] parameterTypes) {
        this.resultRowType = resultsRowType;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.aisColumns = aisColumns;
        this.parameterTypes = parameterTypes;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<PostgresType> getColumnTypes() {
        return columnTypes;
    }

    public RowType getResultRowType() {
        return resultRowType;
    }
    @Override
    public PostgresType[] getParameterTypes() {
        return parameterTypes;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        if (params) {
            PostgresType[] parameterTypes = getParameterTypes();
            messenger.beginMessage(PostgresMessages.PARAMETER_DESCRIPTION_TYPE.code());
            if (parameterTypes == null) {
                messenger.writeShort(0);
            }
            else {
                messenger.writeShort((short)parameterTypes.length);
                for (PostgresType type : parameterTypes) {
                    messenger.writeInt(type.getOid());
                }
            }
            messenger.sendMessage();
        }
        List<PostgresType> columnTypes = getColumnTypes();
        if (columnTypes == null) {
            if (!always) return;
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
        }
        else {
            messenger.beginMessage(PostgresMessages.ROW_DESCRIPTION_TYPE.code());
            List<String> columnNames = getColumnNames();
            int ncols = columnTypes.size();
            messenger.writeShort(ncols);
            for (int i = 0; i < ncols; i++) {
                PostgresType type = columnTypes.get(i);
                Column aisColumn = aisColumns.get(i);
                messenger.writeString(columnNames.get(i)); // attname
                messenger.writeInt((aisColumn == null) ? 0 : aisColumn.getTable().getTableId());    // attrelid
                messenger.writeShort((aisColumn == null) ? 0 : aisColumn.getPosition());  // attnum
                messenger.writeInt(type.getOid()); // atttypid
                messenger.writeShort(type.getLength()); // attlen
                messenger.writeInt(type.getModifier()); // atttypmod
                messenger.writeShort(context.isColumnBinary(i) ? 1 : 0);
            }
        }
        messenger.sendMessage();
    }
    
    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresRowOutputter(context, this);
    }
}
