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

import static com.foundationdb.sql.pg.PostgresJsonCompiler.JsonResultColumn;

import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.Quote;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkResultSet;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;

import java.util.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresJsonOutputter extends PostgresOutputter<Row>
{
    private List<JsonResultColumn> resultColumns;
    private PostgresType valueType;
    
    public PostgresJsonOutputter(PostgresQueryContext context, 
                                 PostgresDMLStatement statement,
                                 List<JsonResultColumn> resultColumns,
                                 PostgresType valueType) {
        super(context, statement);
        this.resultColumns = resultColumns;
        this.valueType = valueType;
    }

    @Override
    public void beforeData() throws IOException {
        if (context.getServer().getOutputFormat() == PostgresServerSession.OutputFormat.JSON_WITH_META_DATA)
            outputMetaData();
    }
    
    @Override
    public void output(Row row) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(1);
        encoder.reset();
        outputRow(row, resultColumns);
        ByteArrayOutputStream bytes = encoder.getByteStream();
        messenger.writeInt(bytes.size());
        messenger.writeByteStream(bytes);
        messenger.sendMessage();
    }

    protected void outputRow(Row row, List<JsonResultColumn> resultColumns)
            throws IOException {
        encoder.appendString("{");
        AkibanAppender appender = encoder.getAppender();
        int ncols = resultColumns.size();
        for (int i = 0; i < ncols; i++) {
            JsonResultColumn resultColumn = resultColumns.get(i);
            encoder.appendString((i == 0) ? "\"" : ",\"");
            Quote.DOUBLE_QUOTE.append(appender, resultColumn.getName());
            encoder.appendString("\":");
            ValueSource value = row.value(i);
            TInstance columnTInstance = resultColumn.getType();
            if (columnTInstance.typeClass() instanceof AkResultSet) {
                outputNestedResultSet((Cursor)value.getObject(),
                                      resultColumn.getNestedResultColumns());
            }
            else {
                FormatOptions options = context.getServer().getFormatOptions();
                columnTInstance.formatAsJson(value, appender, options);
            }
        }
        encoder.appendString("}");
    }

    protected void outputNestedResultSet(Cursor cursor, 
                                         List<JsonResultColumn> resultColumns) 
            throws IOException {
        encoder.appendString("[");
        try {
            Row row;
            boolean first = true;
            while ((row = cursor.next()) != null) {
                if (first)
                    first = false;
                else
                    encoder.appendString(",");
                outputRow(row, resultColumns);
            }
        }
        finally {
            if (!cursor.isClosed()) {
                cursor.close();
            }
        }
        encoder.appendString("]");
    }

    public void outputMetaData() throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(1);
        encoder.reset();
        outputMetaData(resultColumns);
        ByteArrayOutputStream bytes = encoder.getByteStream();
        messenger.writeInt(bytes.size());
        messenger.writeByteStream(bytes);
        messenger.sendMessage();
    }

    public void outputMetaData(List<JsonResultColumn> resultColumns) throws IOException {
        AkibanAppender appender = encoder.getAppender();
        encoder.appendString("[");
        boolean first = true;
        for (JsonResultColumn resultColumn : resultColumns) {
            if (first)
                first = false;
            else
                encoder.appendString(",");
            encoder.appendString("{\"name\":\"");
            Quote.DOUBLE_QUOTE.append(appender, resultColumn.getName());
            encoder.appendString("\"");
            if (resultColumn.getNestedResultColumns() != null) {
                encoder.appendString(",\"columns\":");
                outputMetaData(resultColumn.getNestedResultColumns());
            }
            else {
                if (resultColumn.getPostgresType() != null) {
                    encoder.appendString(",\"oid\":");
                    encoder.getWriter().print(resultColumn.getPostgresType().getOid());
                }
                if (resultColumn.getSqlType() != null) {
                    DataTypeDescriptor type = resultColumn.getSqlType();
                    encoder.appendString(",\"type\":\"");
                    Quote.DOUBLE_QUOTE.append(appender, type.toString());
                    encoder.appendString("\"");
                    TypeId typeId = type.getTypeId();
                    if (typeId.isDecimalTypeId()) {
                        encoder.appendString(",\"precision\":");
                        encoder.getWriter().print(type.getPrecision());
                        encoder.appendString(",\"scale\":");
                        encoder.getWriter().print(type.getScale());
                    }
                    else if (typeId.variableLength()) {
                        encoder.appendString(",\"length\":");
                        encoder.getWriter().print(type.getMaximumWidth());
                    }
                }
            }
            encoder.appendString("}");
        }
        encoder.appendString("]");
    }
}
