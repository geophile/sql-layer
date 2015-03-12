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
package com.foundationdb.server.test.mt;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.ServiceHolder;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static com.foundationdb.qp.operator.API.delete_Returning;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.insert_Returning;
import static com.foundationdb.qp.operator.API.update_Returning;
import static com.foundationdb.qp.operator.API.valuesScan_Default;

// Extend ITBase for the miscellaneous Row/Operator test helpers
public abstract class MTBase extends ITBase implements ServiceHolder
{
    @Rule
    public final TestRule FAILED_WATCHMAN = new TestWatcher() {
        @Override
        public void failed(Throwable e, Description description) {
            System.err.printf("Ran with DELAY_BEFORE: %s, DELAY_DDL_STAGE: %s, DELAY_THREAD_STAGE: %s\n",
                              ConcurrentTestBuilderImpl.DELAY_BEFORE,
                              ConcurrentTestBuilderImpl.DELAY_DDL_STAGE,
                              ConcurrentTestBuilderImpl.DELAY_THREAD_STAGE);
        }
    };


    public MTBase() {
        super("MT");
    }

    //
    // MTBase
    //

    protected List<Row> runPlanTxn(final OperatorCreator creator) {
        return txnService().run(session(), new Callable<List<Row>>() {
            @Override
            public List<Row> call() throws Exception {
                Schema schema = SchemaCache.globalSchema(ais());
                Operator plan = creator.create(schema);
                StoreAdapter adapter = store().createAdapter(session());
                QueryContext context = new SimpleQueryContext(adapter, serviceManager());
                QueryBindings bindings = context.createBindings();
                List<Row> rows = new ArrayList<>();
                Cursor cursor = API.cursor(plan, context, bindings);
                cursor.openTopLevel();
                try {
                    Row row;
                    while((row = cursor.next()) != null) {
                        rows.add(row);
                    }
                } finally {
                    cursor.closeTopLevel();
                }
                return rows;
            }
        });
    }

    protected static List<Row> remove(List<Row> rows, int index) {
        return remove(rows, rows.get(index));
    }

    protected static List<Row> remove(List<Row> rows, Row row) {
        List<Row> newRows = new ArrayList<>(rows);
        newRows.remove(row);
        return newRows;
    }

    protected static List<Row> combine(List<Row> rows, Row newRow) {
        List<Row> newRows = new ArrayList<>(rows);
        newRows.add(newRow);
        return newRows;
    }

    protected static List<Row> insert(List<Row> rows, int index, Row newRow) {
        List<Row> newRows = new ArrayList<>(rows);
        newRows.add(index, newRow);
        return newRows;
    }

    protected static List<Row> replace(List<Row> rows, int index, Row newRow) {
        List<Row> newRows = new ArrayList<>(rows);
        newRows.set(index, newRow);
        return newRows;
    }

    protected static OperatorCreator groupScanCreator(final int tID) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                return groupScan_Default(schema.tableRowType(tID).table().getGroup());
            }
        };
    }

    protected static OperatorCreator indexScanCreator(final int tID, final int iID) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                return API.indexScan_Default(schema.tableRowType(tID).indexRowType(iID));
            }
        };
    }

    protected static OperatorCreator indexScanCreator(final int tID, final String indexName) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                TableRowType tableRowType = schema.tableRowType(tID);
                Index index = tableRowType.table().getIndex(indexName);
                return API.indexScan_Default(tableRowType.indexRowType(index));
            }
        };
    }

    protected static OperatorCreator groupIndexScanCreator(final int tID, final String indexName) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                TableRowType tableType = schema.tableRowType(tID);
                Index index = schema.tableRowType(tID).table().getGroup().getIndex(indexName);
                IndexRowType indexType = schema.indexRowType(index);
                return API.indexScan_Default(indexType, false, IndexKeyRange.unbounded(indexType), tableType);
            }
        };
    }

    protected static OperatorCreator insertCreator(final int tID, final Row newRow) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType rowType = schema.tableRowType(tID);
                return insert_Returning(valuesScan_Default(bindableRows(newRow), rowType));
            }
        };
    }

    protected static OperatorCreator updateCreator(final int tID, final Row oldRow, final Row newRow) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType rowType = schema.tableRowType(tID);
                return update_Returning(
                    valuesScan_Default(bindableRows(oldRow), rowType),
                    new UpdateFunction() {
                        @Override
                        public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
                            return newRow;
                        }
                    });
            }
        };
    }

    protected static OperatorCreator deleteCreator(final int tID, final Row row) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType rowType = schema.tableRowType(tID);
                return delete_Returning(valuesScan_Default(bindableRows(row), rowType), false);
            }
        };
    }


    //
    // ServiceHolder
    //

    @Override
    public TransactionService getTransactionService() {
        return txnService();
    }

    @Override
    public SchemaManager getSchemaManager() {
        return serviceManager().getSchemaManager();
    }

    @Override
    public Store getStore() {
        return store();
    }

    @Override
    public DDLFunctions getDDLFunctions() {
        return ddl();
    }

    @Override
    public Session createSession() {
        return createNewSession();
    }
}
