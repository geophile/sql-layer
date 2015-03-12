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
package com.foundationdb.sql.embedded;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.ExecutionBase;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.ImmutableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class ExecutableModifyOperatorStatement extends ExecutableOperatorStatement
{
    private static final Logger logger = LoggerFactory.getLogger(ExecutableModifyOperatorStatement.class);

    protected ExecutableModifyOperatorStatement(Operator resultOperator,
                                                long aisGeneration,
                                                JDBCResultSetMetaData resultSetMetaData, 
                                                JDBCParameterMetaData parameterMetaData) {
        super(resultOperator, aisGeneration, resultSetMetaData, parameterMetaData);
    }
    
    @Override
    public StatementTypes getStatementType() {
        return StatementTypes.DML_STMT;
    }

    @Override
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
        int updateCount = 0;
        SpoolCursor returningRows = null;
        if (resultSetMetaData != null)
            // If there are results, we need to read them all now to get the update
            // count right and have this all happen even if the caller
            // does not read all of the generated keys.
            returningRows = new SpoolCursor();
        Cursor cursor = null;
        RuntimeException runtimeException = null;
        try {
            cursor = API.cursor(resultOperator, context, bindings);
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                updateCount++;
                if (returningRows != null) {
                    returningRows.add(row);
                }
            }
        }
        catch (RuntimeException ex) {
            runtimeException = ex;
        }
        finally {
            try {
                if (cursor != null) {
                    cursor.closeTopLevel();
                }
            }
            catch (RuntimeException ex) {
                if (runtimeException == null)
                    runtimeException = ex;
                else
                    logger.warn("Error cleaning up cursor with exception already pending", ex);
            }
            if (runtimeException != null)
                throw runtimeException;
        }
        if (returningRows != null)
            returningRows.open(); // Done filling.
        return new ExecuteResults(updateCount, returningRows);
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.WRITE;
    }

    static class SpoolCursor  extends RowCursorImpl  {
        private List<Row> rows = new ArrayList<>();
        private Iterator<Row> iterator;
        private enum State { FILLING, EMPTYING}
        private State spoolState;
        
        public SpoolCursor() {
            spoolState = State.FILLING;
        }

        public void add(Row row) {
            assert (spoolState == State.FILLING);
            if (row.isBindingsSensitive())
                // create a copy of this row, and hold it instead
                row = ImmutableRow.buildImmutableRow(row);
            rows.add(row);
        }

        @Override
        public void open() {
            super.open();
            iterator = rows.iterator();
            spoolState = State.EMPTYING;
        }        

        @Override
        public Row next() {
            if (ExecutionBase.CURSOR_LIFECYCLE_ENABLED) {
                CursorLifecycle.checkIdleOrActive(this);
            }
            assert (spoolState == State.EMPTYING);
            if (iterator.hasNext()) {
                Row row = iterator.next();
                return row;
            }
            else {
                setIdle();
                return null;
            }
        }
    }
}


