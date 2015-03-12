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
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerCallInvocation;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import java.util.List;
import java.io.IOException;

public class PostgresLoadableDirectObjectPlan extends PostgresDMLStatement
                                       implements PostgresCursorGenerator<DirectObjectCursor>
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadableDirectObjectPlan: execute shared");

    private ServerCallInvocation invocation;
    private DirectObjectPlan plan;
    private DirectObjectPlan.OutputMode outputMode;

    protected PostgresLoadableDirectObjectPlan(LoadableDirectObjectPlan loadablePlan,
                                               ServerCallInvocation invocation,
                                               List<String> columnNames, List<PostgresType> columnTypes, List<Column> aisColumns,
                                               PostgresType[] parameterTypes)
    {
        super.init(null, columnNames, columnTypes, aisColumns, parameterTypes);
        this.invocation = invocation;
        plan = loadablePlan.plan();
        outputMode = plan.getOutputMode();
    }
    
    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    public TransactionMode getTransactionMode() {
        switch (plan.getTransactionMode()) {
        case READ_ONLY:
            return TransactionMode.READ;
        case READ_WRITE:
            return TransactionMode.WRITE;
        case NONE:
        default:
            return TransactionMode.NONE;
        }
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        // The copy cases will be handled below.
        if (outputMode == DirectObjectPlan.OutputMode.TABLE)
            super.sendDescription(context, always, params);
    }

    @Override
    public boolean canSuspend(PostgresServerSession server) {
        return true;
    }

    @Override
    public DirectObjectCursor openCursor(PostgresQueryContext context, QueryBindings bindings) {
        DirectObjectCursor cursor = plan.cursor(context, bindings);
        cursor.open();
        return cursor;
    }

    public void closeCursor(DirectObjectCursor cursor) {
        if (cursor != null) {
            cursor.close();
        }
    }
    
    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        DirectObjectCursor cursor = null;
        PostgresOutputter<List<?>> outputter = null;
        PostgresDirectObjectCopier copier = null;
        bindings = PostgresLoadablePlan.setParameters(bindings, invocation);
        ServerCallContextStack stack = ServerCallContextStack.get();
        boolean suspended = false, success = false;
        stack.push(context, invocation);
        try {
            cursor = context.startCursor(this, bindings);
            switch (outputMode) {
            case TABLE:
                outputter = new PostgresDirectObjectOutputter(context, this);
                break;
            case COPY:
            case COPY_WITH_NEWLINE:
                outputter = copier = new PostgresDirectObjectCopier(context, this, (outputMode == DirectObjectPlan.OutputMode.COPY_WITH_NEWLINE));
                copier.respond();
                break;
            }
            if (cursor != null) {
                List<?> row;
                while ((row = cursor.next()) != null) {
                    if (row.isEmpty()) {
                        messenger.flush();
                    }
                    else {
                        outputter.output(row);
                        nrows++;
                    }
                    if ((maxrows > 0) && (nrows >= maxrows)) {
                        suspended = true;
                        break;
                    }
                }
            }
            if (copier != null) {
                copier.done();
            }
            success = true;
        }
        finally {
            suspended = context.finishCursor(this, cursor, nrows, suspended);
            stack.pop(context, invocation, success);
        }
        if (suspended) {
            return portalSuspended(nrows);
        }
        else if (copier != null) {
            // Make CopyManager happy.
            return commandComplete("COPY", nrows);
        }

        else {        
            return commandComplete("CALL " + nrows, nrows);
        }
    }

}
