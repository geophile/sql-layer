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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.virtualadapter.VirtualAdapter;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.externaldata.JsonRowWriter.WriteTableRow;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.AkibanAppender;
import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExternalDataServiceImpl implements ExternalDataService, Service {
    protected final ConfigurationService configService;
    protected final DXLService dxlService;
    protected final Store store;
    protected final TransactionService transactionService;
    protected final ServiceManager serviceManager;
    
    private static final Logger logger = LoggerFactory.getLogger(ExternalDataServiceImpl.class);

    public static final CacheValueGenerator<PlanGenerator> CACHED_PLAN_GENERATOR =
            new CacheValueGenerator<PlanGenerator>() {
                @Override
                public PlanGenerator valueFor(AkibanInformationSchema ais) {
                    return new PlanGenerator(ais);
                }
            };


    @Inject
    public ExternalDataServiceImpl(ConfigurationService configService,
                                   DXLService dxlService, Store store,
                                   TransactionService transactionService,
                                   ServiceManager serviceManager) {
        this.configService = configService;
        this.dxlService = dxlService;
        this.store = store;
        this.transactionService = transactionService;
        this.serviceManager = serviceManager;

    }

    private Table getTable(AkibanInformationSchema ais, String schemaName, String tableName) {
        Table table = ais.getTable(schemaName, tableName);
        if (table == null) {
            // TODO: Consider sending in-band as JSON.
            throw new NoSuchTableException(schemaName, tableName);
        }
        return table;
    }

    private StoreAdapter getAdapter(Session session, Table table) {
        if (table.isVirtual())
            return new VirtualAdapter(session, configService);
        return store.createAdapter(session);
    }

    private TypesTranslator getTypesTranslator() {
        return dxlService.ddlFunctions().getTypesTranslator();
    }

    private void dumpAsJson(Session session,
                            PrintWriter writer,
                            Table table,
                            List<List<Object>> keys,
                            int depth,
                            boolean withTransaction,
                            Operator plan,
                            FormatOptions options) {
        StoreAdapter adapter = getAdapter(session, table);
        QueryContext queryContext = new SimpleQueryContext(adapter) {
                @Override
                public ServiceManager getServiceManager() {
                    return serviceManager;
                }
            };
        QueryBindings queryBindings = queryContext.createBindings();
        JsonRowWriter json = new JsonRowWriter(new TableRowTracker(table, depth));
        WriteTableRow rowWriter = new WriteTableRow();
        AkibanAppender appender = AkibanAppender.of(writer);
        boolean transaction = false;
        Cursor cursor = null;
        try {
            if (withTransaction) {
                transactionService.beginTransaction(session);
                transaction = true;
            }
            cursor = API.cursor(plan, queryContext, queryBindings);
            appender.append("[");
            boolean begun = false;

            if (keys == null) {
                begun = json.writeRows(cursor, appender, "\n", rowWriter, options);
            } else {
                for (List<Object> key : keys) {
                    for (int i = 0; i < key.size(); i++) {
                        ValueSource value = ValueSources.fromObject(key.get(i));
                        queryBindings.setValue(i, value);
                    }
                    if (json.writeRows(cursor, appender, begun ? ",\n" : "\n", rowWriter, options))
                        begun = true;
                }
            }

            appender.append(begun ? "\n]" : "]");
            if (withTransaction) {
                transactionService.commitTransaction(session);
                transaction = false;
            }
        }
        finally {
            if (cursor != null && !cursor.isClosed())
                cursor.closeTopLevel();
            if (transaction)
                transactionService.rollbackTransaction(session);
        }
    }

    /* ExternalDataService */

    @Override
    public void dumpAllAsJson(Session session, PrintWriter writer,
                              String schemaName, String tableName,
                              int depth, boolean withTransaction, FormatOptions options) {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        Table table = getTable(ais, schemaName, tableName);
        logger.debug("Writing all of {}", table);
        PlanGenerator generator = ais.getCachedValue(this, CACHED_PLAN_GENERATOR);
        Operator plan = generator.generateScanPlan(table);
        dumpAsJson(session, writer, table, null, depth, withTransaction, plan, options);
    }

    @Override
    public void dumpBranchAsJson(Session session, PrintWriter writer,
                                 String schemaName, String tableName, 
                                 List<List<Object>> keys, int depth,
                                 boolean withTransaction, FormatOptions options) {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        Table table = getTable(ais, schemaName, tableName);
        logger.debug("Writing from {}: {}", table, keys);
        PlanGenerator generator = ais.getCachedValue(this, CACHED_PLAN_GENERATOR);
        Operator plan = generator.generateBranchPlan(table);
        dumpAsJson(session, writer, table, keys, depth, withTransaction, plan, options);
    }

    @Override
    public void dumpBranchAsJson(Session session, PrintWriter writer,
                                 String schemaName, String tableName, 
                                 Operator scan, RowType scanType, int depth,
                                 boolean withTransaction, FormatOptions options) {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        Table table = getTable(ais, schemaName, tableName);
        logger.debug("Writing from {}: {}", table, scan);
        PlanGenerator generator = ais.getCachedValue(this, CACHED_PLAN_GENERATOR);
        Operator plan = generator.generateBranchPlan(table, scan, scanType);
        dumpAsJson(session, writer, table, Collections.singletonList(Collections.emptyList()), depth, withTransaction, plan, options);
    }

    @Override
    public long loadTableFromCsv(Session session, InputStream inputStream, 
                                 CsvFormat format, long skipRows,
                                 Table toTable, List<Column> toColumns,
                                 long commitFrequency, int maxRetries,
                                 QueryContext context) 
            throws IOException {
        CsvRowReader reader = new CsvRowReader(toTable, toColumns, inputStream, format,
                                               context, getTypesTranslator());
        if (skipRows > 0)
            reader.skipRows(skipRows);
        return loadTableFromRowReader(session, inputStream, reader, 
                                      commitFrequency, maxRetries);
    }

    @Override
    public long loadTableFromMysqlDump(Session session, InputStream inputStream, 
                                       String encoding,
                                       Table toTable, List<Column> toColumns,
                                       long commitFrequency, int maxRetries,
                                       QueryContext context) 
            throws IOException {
        MysqlDumpRowReader reader = new MysqlDumpRowReader(toTable, toColumns,
                                                           inputStream, encoding, 
                                                           context, getTypesTranslator());
        return loadTableFromRowReader(session, inputStream, reader, 
                                      commitFrequency, maxRetries);
    }

    protected long loadTableFromRowReader(Session session, 
                                          InputStream inputStream, RowReader reader, 
                                          long commitFrequency, int maxRetries)
            throws IOException {
        long pending = 0, total = 0;
        List<Row> rows = maxRetries > 0 ? new ArrayList<Row>() : null;
        boolean transaction = false;
        try {
            Row row;
            do {
                if (!transaction) {
                    // A transaction is needed, even to read rows, because of auto
                    // increment.
                    transactionService.beginTransaction(session);
                    transaction = true;
                }
                row = reader.nextRow();
                logger.trace("Read row: {}", row);
                if (row != null) {
                    if (rows != null) {
                        rows.add(row);
                    }
                    total++;
                    pending++;
                }
                boolean commit = false;
                if (row == null) {
                    commit = true;
                }
                else if (commitFrequency == COMMIT_FREQUENCY_PERIODICALLY) {
                    commit = transactionService.shouldPeriodicallyCommit(session);
                }
                else if (commitFrequency != COMMIT_FREQUENCY_NEVER) {
                    commit = (pending >= commitFrequency);
                }
                Exception retryException = null;
                int sessionCounter = -1;
                for (int i = 0; i <= maxRetries; i++) {
                    try {
                        retryHook(session, i, maxRetries, retryException);
                        if (i == 0) {
                            if (row != null) {
                                store.writeRow(session, row, null, null);
                            }
                        }
                        else {
                            logger.debug("retry #{} from {}", i, retryException);
                            if (!transaction) {
                                transactionService.beginTransaction(session);
                                transaction = true;
                            }
                            if (transactionService.checkSucceeded(session,
                                                                  retryException,
                                                                  sessionCounter)) {
                                logger.debug("transaction had succeeded");
                                rows.clear();
                                break;
                            }
                            // If another exception occurs before here, that is,
                            // while setting up or checking, we repeat check with
                            // original exception and counter. Once check succeeds
                            // but does not pass, we set to get another one.
                            retryException = null;
                            // And errors before another commit cannot be spurious.
                            sessionCounter = -1;
                            for (Row aRow : rows) {
                                store.writeRow(session, aRow, null, null);
                            }
                        }
                        if (commit) {
                            if (i == 0) {
                                logger.debug("Committing {} rows", pending);
                                pending = 0;
                            }
                            sessionCounter = transactionService.markForCheck(session);
                            transaction = false;
                            transactionService.commitTransaction(session);
                            if (rows != null) {
                                rows.clear();
                            }
                        }
                        break;
                    }
                    catch (InvalidOperationException ex) {
                        if ((i >= maxRetries) ||
                            !ex.getCode().isRollbackClass()) {
                            throw ex;
                        }
                        if (retryException == null) {
                            retryException = ex;
                        }
                        if (transaction) {
                            transaction = false;
                            transactionService.rollbackTransaction(session);
                        }
                    }
                }
            } while (row != null);
        }
        finally {
            if (transaction) {
                transactionService.rollbackTransaction(session);
            }
        }
        return total;
    }

    
    // For testing by failure injection.
    protected void retryHook(Session session, int i, int maxRetries,
                             Exception retryException) {
    }

    /* Service */
    
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }

}
