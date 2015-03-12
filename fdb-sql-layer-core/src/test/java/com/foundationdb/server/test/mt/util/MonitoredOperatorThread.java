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
package com.foundationdb.server.test.mt.util;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MonitoredOperatorThread extends MonitoredThread
{
    private final OperatorCreator operatorCreator;
    private final boolean retryOnRollback;


    public MonitoredOperatorThread(String name,
                                   ServiceHolder services,
                                   OperatorCreator operatorCreator,
                                   ThreadMonitor monitor,
                                   Set<Stage> stageMarks,
                                   boolean retryOnRollback) {
        super(name, services, monitor, stageMarks);
        this.operatorCreator = operatorCreator;
        this.retryOnRollback = retryOnRollback;
    }

    //
    // MonitoredThread
    //

    @Override
    protected boolean doRetryOnRollback() {
        return retryOnRollback;
    }

    @Override
    protected void runInternal(Session session) {
        to(Stage.PRE_BEGIN);
        boolean success = false;
        getServiceHolder().getTransactionService().beginTransaction(session);
        // To ensure our desired ordering
        getServiceHolder().getTransactionService().getTransactionStartTimestamp(session);
        to(Stage.POST_BEGIN);
        try {
            while(!success) {
                to(Stage.PRE_SCAN);
                Schema schema = SchemaCache.globalSchema(getServiceHolder().getSchemaManager().getAis(session));
                Operator plan = operatorCreator.create(schema);
                StoreAdapter adapter = getServiceHolder().getStore().createAdapter(session);
                QueryContext context = new SimpleQueryContext(adapter);
                QueryBindings bindings = context.createBindings();
                Cursor cursor = API.cursor(plan, context, bindings);
                cursor.openTopLevel();
                try {
                    Row row;
                    while((row = cursor.next()) != null) {
                        getScannedRows().add(row);
                        switch(getScannedRows().size()) {
                            case 1: to(Stage.SCAN_FIRST_ROW); break;
                            case 2: to(Stage.SCAN_SECOND_ROW); break;
                        }
                    }
                } finally {
                    cursor.closeTopLevel();
                }
                to(Stage.POST_SCAN);

                to(Stage.PRE_COMMIT);
                getServiceHolder().getTransactionService().commitTransaction(session);
                success = true;
                to(Stage.POST_COMMIT);
            }
        } finally {
            if(!success) {
                to(Stage.PRE_ROLLBACK);
                getServiceHolder().getTransactionService().rollbackTransactionIfOpen(session);
                to(Stage.POST_ROLLBACK);
            }
        }
    }
}
