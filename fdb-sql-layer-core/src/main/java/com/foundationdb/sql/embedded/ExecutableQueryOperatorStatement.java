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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.sql.optimizer.plan.CostEstimate;

class ExecutableQueryOperatorStatement extends ExecutableOperatorStatement
{
    private CostEstimate costEstimate;
    private static final Logger LOG = LoggerFactory.getLogger(ExecutableQueryOperatorStatement.class);
    
    protected ExecutableQueryOperatorStatement(Operator resultOperator,
                                               long aisGeneration,
                                               JDBCResultSetMetaData resultSetMetaData,
                                               JDBCParameterMetaData parameterMetaData,
                                               CostEstimate costEstimate) {
        super(resultOperator, aisGeneration, resultSetMetaData, parameterMetaData);
        this.costEstimate = costEstimate;
    }

    @Override
    public StatementTypes getStatementType() {
        return StatementTypes.SELECT;
    }
    
    @Override
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
        Cursor cursor = null;
        try {
            cursor = API.cursor(resultOperator, context, bindings);
            cursor.openTopLevel();
            ExecuteResults result = new ExecuteResults(cursor);
            cursor = null;
            return result;
        } catch (RuntimeException e) {
            LOG.error("caught error: {}", e.toString());
            cursor = null;
            throw e;
        }       
        finally {
            if (cursor != null) {
                cursor.closeTopLevel();
            }
        }
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public long getEstimatedRowCount() {
        if (costEstimate == null)
            return -1;
        else
            return costEstimate.getRowCount();
    }

}
