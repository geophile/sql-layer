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
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonRowWriter
{
    private static final Logger logger = LoggerFactory.getLogger(JsonRowWriter.class);

    private final RowTracker tracker;

    public JsonRowWriter(RowTracker tracker) {
        this.tracker = tracker;
    }

    public boolean writeRows(Cursor cursor, AkibanAppender appender, String prefix, WriteRow rowWriter, FormatOptions options) {
        try {
            cursor.openTopLevel();
            return writeRowsFromOpenCursor(cursor, appender, prefix, rowWriter, options);
        }
        finally {
            cursor.closeTopLevel();
        }
    }

    public boolean writeRowsFromOpenCursor(RowCursor cursor, AkibanAppender appender, String prefix, WriteRow rowWriter, FormatOptions options) {
        tracker.reset();
        final int minDepth = tracker.getMinDepth();
        final int maxDepth = tracker.getMaxDepth();
        int depth = minDepth - 1;
        Row row;
        while ((row = cursor.next()) != null) {
            logger.trace("Row {}", row);
            tracker.beginRow(row);
            int rowDepth = tracker.getRowDepth();
            boolean begun = false;
            if (depth >= rowDepth) {
                if (tracker.isSameRowType())
                    begun = true;
                do {
                    appender.append((depth > rowDepth || !begun) ? "}]" : "}");
                    depth--;
                    tracker.popRowType();
                } while (depth >= rowDepth);
            }
            if (rowDepth > maxDepth)
                continue;
            assert (rowDepth == depth+1);
            depth = rowDepth;
            tracker.pushRowType();
            if (begun) {
                appender.append(',');
            }
            else if (depth > minDepth) {
                appender.append(",\"");
                appender.append(tracker.getRowName());
                appender.append("\":[");
            }
            else {
                appender.append(prefix);
            }
            appender.append('{');
            rowWriter.write(row, appender, options);
        }
        if (depth < minDepth)
            return false;       // Cursor was empty = not found.
        do {
            appender.append((depth > minDepth) ? "}]" : "}");
            depth--;
            tracker.popRowType();
        } while (depth >= minDepth);
        return true;
    }

    public static void writeValue(String name, ValueSource value, AkibanAppender appender, 
                                  boolean first, FormatOptions options) {
        if(!first) {
            appender.append(',');
        }
        appender.append('"');
        appender.append(name);
        appender.append("\":");
        value.getType().formatAsJson(value, appender, options);
    }

    /**
     * Write the name:value pairs of the data from a row into Json format.
     * Current implementations take names from the table columns or the
     * table's primary key columns. 
     * @author tjoneslo
     */
    public interface WriteRow {
        public void write(Row row, AkibanAppender appender, FormatOptions options);

    }
    
    public static class WriteTableRow implements WriteRow {
        @Override
        public void write(Row row, AkibanAppender appender, FormatOptions options) {
            List<Column> columns = row.rowType().table().getColumns();
            for (int i = 0; i < columns.size(); i++) {
                writeValue(columns.get(i).getName(), row.value(i), appender, i == 0, options);
             }
        }
    }

    public static class WriteCapturePKRow implements WriteRow {
        private Map<Column, ValueSource> pkValues = new HashMap<>();

        @Override
        public void write(Row row, AkibanAppender appender, FormatOptions options) {
            // tables with hidden PK (noPK tables) return no values
            if (row.rowType().table().getPrimaryKey() == null) return;
            
            List<IndexColumn> columns = row.rowType().table().getPrimaryKey().getIndex().getKeyColumns();
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i).getColumn();
                writeValue(column.getName(), row.value(column.getPosition()), appender, i == 0, options);
                pkValues.put(column, row.value(column.getPosition()));
            }
        }

        public Map<Column, ValueSource> getPKValues() {
            return pkValues;
        }
    }
}
