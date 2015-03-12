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
import com.foundationdb.sql.optimizer.plan.CostEstimate;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;

import java.io.IOException;
import java.util.List;

import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An SQL modifying DML statement transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresModifyOperatorStatement extends PostgresBaseOperatorStatement
                                             implements PostgresCursorGenerator<Cursor>
{
    private String statementType;
    private Operator resultOperator;
    private boolean outputResult;
    private boolean putInCache = true;
    private CostEstimate costEstimate;

    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresBaseStatement: execute exclusive");
    private static final Logger LOG = LoggerFactory.getLogger(PostgresModifyOperatorStatement.class);

    public PostgresModifyOperatorStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
    }

    public void init(String statementType,
                     Operator resultsOperator,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate,
                     boolean putInCache) {
        super.init(parameterTypes);
        this.statementType = statementType;
        this.resultOperator = resultsOperator;
        this.costEstimate = costEstimate;
        outputResult = false;
        this.putInCache = putInCache;
    }
    
    public void init(String statementType,
                     Operator resultOperator,
                     RowType resultRowType,
                     List<String> columnNames,
                     List<PostgresType> columnTypes,
                     List<Column> aisColumns,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate,
                     boolean putInCache) {
        super.init(resultRowType, columnNames, columnTypes, aisColumns, parameterTypes);
        this.statementType = statementType;
        this.resultOperator = resultOperator;
        this.costEstimate = costEstimate;
        outputResult = true;
        this.putInCache = putInCache;
    }

    public boolean isInsert() {
        return "INSERT".equals(statementType);
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.WRITE;
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
    public boolean putInCache() {
        return putInCache;
    }

    @Override
    public boolean canSuspend(PostgresServerSession server) {
        return false;           // See below.
    }

    @Override
    public Cursor openCursor(PostgresQueryContext context, QueryBindings bindings) {
        Cursor cursor = API.cursor(resultOperator, context, bindings);
        cursor.openTopLevel();
        return cursor;
    }

    @Override
    public void closeCursor(Cursor cursor) {
        if (cursor != null) {
            cursor.closeTopLevel();
        }
    }
    
    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.getSessionMonitor().countEvent(StatementTypes.DML_STMT);
        PostgresMessenger messenger = server.getMessenger();
        int rowsModified = 0;
        if (resultOperator != null) {
            Cursor cursor = null;
            IOException exceptionDuringExecution = null;
            try {
                preExecute(context, DXLFunction.UNSPECIFIED_DML_WRITE);
                cursor = openCursor(context, bindings);
                PostgresOutputter<Row> outputter = null;
                if (outputResult) {
                    outputter = getRowOutputter(context);
                    outputter.beforeData();
                }
                Row row;
                while ((row = cursor.next()) != null) {
                    assert getResultRowType() == null || (row.rowType() == getResultRowType()) : row;
                    if (outputResult) { 
                        outputter.output(row);
                    }
                    rowsModified++;
                    if ((maxrows > 0) && (rowsModified >= maxrows))
                        // Note: do not allow suspending, since the
                        // actual modifying and not just the generated
                        // key output would be incomplete.
                        outputResult = false;
                }
            }
            catch (IOException e) {
                exceptionDuringExecution = e;
            }
            finally {
                RuntimeException exceptionDuringCleanup = null;
                try {
                    closeCursor(cursor);
                }
                catch (RuntimeException e) {
                    exceptionDuringCleanup = e;
                    LOG.error("Caught exception while cleaning up cursor for {0}", resultOperator.describePlan());
                    LOG.error("Exception stack", e);
                }
                finally {
                    postExecute(context, DXLFunction.UNSPECIFIED_DML_WRITE);
                }
                if (exceptionDuringExecution != null) {
                    throw exceptionDuringExecution;
                } else if (exceptionDuringCleanup != null) {
                    throw exceptionDuringCleanup;
                }
            }
        }
        
        //TODO: Find a way to extract InsertNode#statementToString() or equivalent
        return commandComplete(isInsert() ?
                               (statementType + " 0 " + rowsModified) :
                               (statementType + " " + rowsModified));
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
