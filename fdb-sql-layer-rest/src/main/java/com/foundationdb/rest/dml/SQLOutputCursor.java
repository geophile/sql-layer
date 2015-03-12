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
package com.foundationdb.rest.dml;

import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.row.DelegateRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.service.externaldata.GenericRowTracker;
import com.foundationdb.server.service.externaldata.JsonRowWriter;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.sql.embedded.JDBCResultSet;
import com.foundationdb.sql.embedded.JDBCResultSetMetaData;
import com.foundationdb.util.AkibanAppender;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;

public class SQLOutputCursor extends GenericRowTracker implements RowCursor, JsonRowWriter.WriteRow {
    private final Deque<ResultSetHolder> holderStack = new ArrayDeque<>();
    private ResultSetHolder currentHolder;
    private FormatOptions options;

    public SQLOutputCursor(JDBCResultSet rs, FormatOptions options) throws SQLException {
        currentHolder = new ResultSetHolder(rs, null, 0);
        this.options = options;
    }

    //
    // GenericRowTracker
    //

    @Override
    public String getRowName() {
        return currentHolder.name;
    }

    //
    // Cursor
    //

    @Override
    public void open() {
    }

    @Override
    public Row next() {
        if(currentHolder == null) {
            assert holderStack.isEmpty();
            return null;
        }
        try {
            Row row = null;
            if(!holderStack.isEmpty() && holderStack.peek().depth > currentHolder.depth) {
                currentHolder = holderStack.pop();
            }
            if(currentHolder.resultSet.next()) {
                row = currentHolder.resultSet.unwrap(Row.class);
                setDepth(currentHolder.depth);
            } else if(!holderStack.isEmpty()) {
                currentHolder = holderStack.pop();
                row = next();
            } else {
                currentHolder = null;
            }
            return (row != null) ? new RowHolder(row, currentHolder) : null;
        } catch(SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        holderStack.clear();
        currentHolder = null;
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIdle() {
       
    }

    @Override
    public boolean isIdle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isActive() {
        throw new UnsupportedOperationException();
    }

    //
    // JsonRowWriter.WriteRow
    //

    @Override
    public void write(Row row, AkibanAppender appender, FormatOptions options) {
        if(!(row instanceof RowHolder)) {
            throw new IllegalArgumentException("Unexpected row: " + row.getClass());
        }
        try {
            RowHolder rowHolder = (RowHolder)row;
            JDBCResultSet resultSet = rowHolder.rsHolder.resultSet;
            JDBCResultSetMetaData metaData = resultSet.getMetaData();
            boolean begun = false;
            boolean savedCurrent = false;
            for(int col = 1; col <= metaData.getColumnCount(); ++col) {
                String colName = metaData.getColumnLabel(col);
                if(metaData.getNestedResultSet(col) != null) {
                    if(!savedCurrent) {
                        holderStack.push(currentHolder);
                        savedCurrent = true;
                    }
                    JDBCResultSet nested = (JDBCResultSet)resultSet.getObject(col);
                    holderStack.push(new ResultSetHolder(nested, colName, rowHolder.rsHolder.depth + 1));
                } else {
                    ValueSource valueSource = row.value(col - 1);
                    JsonRowWriter.writeValue(colName, valueSource, appender, !begun, options);
                    begun = true;
                }
            }
        } catch(SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class ResultSetHolder {
        public final JDBCResultSet resultSet;
        public final String name;
        public final int depth;

        private ResultSetHolder(JDBCResultSet resultSet, String name, int depth) {
            this.resultSet = resultSet;
            this.name = name;
            this.depth = depth;
        }

        @Override
        public String toString() {
            return name + "(" + depth + ")";
        }
    }

    private static class RowHolder extends DelegateRow {
        public final ResultSetHolder rsHolder;

        public RowHolder(Row delegate, ResultSetHolder rsHolder) {
            super(delegate);
            this.rsHolder = rsHolder;
        }
    }

}
