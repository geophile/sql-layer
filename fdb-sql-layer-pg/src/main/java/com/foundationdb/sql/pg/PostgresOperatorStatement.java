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

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.sql.optimizer.plan.CostEstimate;
import com.foundationdb.qp.operator.*;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.util.*;
import java.io.IOException;

/**
 * An SQL SELECT transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresOperatorStatement extends PostgresBaseOperatorStatement 
                                       implements PostgresCursorGenerator<Cursor>
{
    private Operator resultOperator;
    private CostEstimate costEstimate;

    private static final Logger logger = LoggerFactory.getLogger(PostgresOperatorStatement.class);
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresOperatorStatement: execute shared");

    public PostgresOperatorStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
    }

    public void init(Operator resultOperator,
                     RowType resultRowType,
                     List<String> columnNames,
                     List<PostgresType> columnTypes,
                     List<Column> aisColumns,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate) {
        super.init(resultRowType, columnNames, columnTypes, aisColumns, parameterTypes);
        this.resultOperator = resultOperator;
        this.costEstimate = costEstimate;
    }
    
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

    @Override
    public boolean canSuspend(PostgresServerSession server) {
        return server.isTransactionActive();
    }

    @Override
    public Cursor openCursor(PostgresQueryContext context, QueryBindings bindings) {
        Cursor cursor = API.cursor(resultOperator, context, bindings);
        cursor.openTopLevel();
        return cursor;
    }

    public void closeCursor(Cursor cursor) {
        if (cursor != null) {
            cursor.closeTopLevel();
        }
    }
    
    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.getSessionMonitor().countEvent(StatementTypes.SELECT);
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        Cursor cursor = null;
        IOException exceptionDuringExecution = null;
        InvalidOperationException exceptionFromExecution = null;
        RuntimeException runtimeExDuringExecution = null;
        boolean suspended = false;
        try {
            preExecute(context, DXLFunction.UNSPECIFIED_DML_READ);
            cursor = context.startCursor(this, bindings);
            PostgresOutputter<Row> outputter = getRowOutputter(context);
            outputter.beforeData();
            if (cursor != null) {
                Row row;
                while ((row = cursor.next()) != null) {
                    assert (getResultRowType() == null) || (row.rowType() == getResultRowType()) : row;
                    outputter.output(row);
                    nrows++;
                    if ((maxrows > 0) && (nrows >= maxrows)) {
                        suspended = true;
                        break;
                    }
                }
            }
            outputter.afterData();
        }
        catch (IOException e) {
            exceptionDuringExecution = e;
        } catch (InvalidOperationException e) {
            e.getCode().logAtImportance(logger, 
                    "Caught unexpected InvalidOperationException during execution: {}", 
                    e.getLocalizedMessage(), e);
            exceptionFromExecution = e;
        } catch (RuntimeException e) {
            logger.error("Caught unexpected runtime exception during execution {}", e.getLocalizedMessage());
            runtimeExDuringExecution = e;
        }
        finally {
            RuntimeException exceptionDuringCleanup = null;
            try {
                suspended = context.finishCursor(this, cursor, nrows, suspended);
            } catch (InvalidOperationException e) {
                logger.warn("Caught InvalidOperationException during cleanup: {}", 
                        e.getLocalizedMessage());
                exceptionDuringCleanup = e;
            } catch (RuntimeException e) {
                exceptionDuringCleanup = e;
                logger.error("Caught unexpected exception while cleaning up cursor for {} : {}", resultOperator.describePlan(), e.getLocalizedMessage());
            }
            finally {
                postExecute(context, DXLFunction.UNSPECIFIED_DML_READ);
            }
            if (exceptionDuringExecution != null) {
                throw exceptionDuringExecution;
            } else if (runtimeExDuringExecution != null) {
                throw runtimeExDuringExecution;
            } else if (exceptionFromExecution != null) {
                throw exceptionFromExecution;
            } else if (exceptionDuringCleanup != null) {
                throw exceptionDuringCleanup;
            }
        }
        if (suspended) {
            return portalSuspended(nrows);
        }
        else {
            return commandComplete("SELECT " + nrows, nrows);
        }
    }

    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    public CostEstimate getCostEstimate() {
        return costEstimate;
    }

}
