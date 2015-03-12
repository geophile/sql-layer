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
package com.foundationdb.server.service.dxl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BasicDMLFunctions implements DMLFunctions {
    private final static Logger logger = LoggerFactory.getLogger(BasicDMLFunctions.class);

    private final SchemaManager schemaManager;
    private final Store store;
    private final ListenerService listenerService;

    @Inject
    BasicDMLFunctions(SchemaManager schemaManager, Store store, ListenerService listenerService) {
        this.schemaManager = schemaManager;
        this.store = store;
        this.listenerService = listenerService;
    }

    /**
     * Determine if a Table can be truncated 'quickly' through the Store interface.
     * This is possible if the entire group can be truncated. Specifically, all other
     * tables in the group must have no rows.
     * @param session Session to operation on
     * @param table Table to determine if a fast truncate is possible on
     * @param descendants <code>true</code> to ignore descendants of
     * <code>table</code> in the check
     * @return true if store.truncateGroup() used, false otherwise
     */
    private boolean canFastTruncate(Session session, Table table, boolean descendants) {
        if(!table.getFullTextIndexes().isEmpty()) {
            return false;
        }
        List<Table> tableList = new ArrayList<>();
        tableList.add(table.getGroup().getRoot());
        while(!tableList.isEmpty()) {
            Table aTable = tableList.remove(tableList.size() - 1);
            if(aTable != table) {
                if(aTable.tableStatus().getRowCount(session) > 0) {
                    return false;
                }
            }
            if((aTable != table) || !descendants) {
                for(Join join : aTable.getChildJoins()) {
                    tableList.add(join.getChild());
                }
            }
        }
        if (BasicDDLFunctions.containsBlob(table))
            return false;
        return true;
    }

    @Override
    public void truncateTable(final Session session, final int tableId)
    {
        truncateTable(session, tableId, false);
    }

    @Override
    public void truncateTable(final Session session, final int tableId, final boolean descendants)
    {
        logger.trace("truncating tableId={}", tableId);
        final AkibanInformationSchema ais = schemaManager.getAis(session);
        final Table table = ais.getTable(tableId);

        if(canFastTruncate(session, table, descendants)) {
            store.truncateGroup(session, table.getGroup());
            // All other tables in the group have no rows. Only need to truncate this table.
            for(TableListener listener : listenerService.getTableListeners()) {
                listener.onTruncate(session, table, true);
            }
            return;
        }

        slowTruncate(session, table, descendants);
    }

    private void slowTruncate(Session session, Table table, boolean descendants) {
        final com.foundationdb.qp.rowtype.Schema schema = SchemaCache.globalSchema(table.getAIS());
        final Set<TableRowType> filterTypes;
        if(descendants) {
            filterTypes = new HashSet<>();
            table.visit(new AbstractVisitor() {
                @Override
                public void visit(Table t) {
                    TableRowType rowType = schema.tableRowType(t);
                    assert rowType != null : t;
                    filterTypes.add(rowType);
                }
            });
        } else {
            filterTypes = Collections.singleton(schema.tableRowType(table));
        }

        // We can't do a "fast truncate" for whatever reason so do so with a full scan.
        Operator plan =
            API.delete_Returning(
                API.filter_Default(
                    API.groupScan_Default(table.getGroup()), filterTypes),
                false
            );

        StoreAdapter adapter = store.createAdapter(session);
        QueryContext context = new SimpleQueryContext(adapter);
        com.foundationdb.qp.operator.Cursor cursor = API.cursor(plan, context, context.createBindings());
        cursor.openTopLevel();
        try {
            Row row;
            do {
                row = cursor.next();
            }
            while(row != null);
        } finally {
            cursor.closeTopLevel();
        }

        for(TableListener listener : listenerService.getTableListeners()) {
            listener.onTruncate(session, table, false);
        }
    }
}
