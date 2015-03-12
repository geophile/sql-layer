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
package com.foundationdb.server;

import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.virtualadapter.VirtualScanFactory;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.MemoryTransaction;
import com.foundationdb.server.store.MemoryTransactionService;
import com.foundationdb.server.store.format.MemoryStorageDescription;

import java.util.HashMap;
import java.util.Map;

import static com.foundationdb.server.store.MemoryStore.join;
import static com.foundationdb.server.store.MemoryStore.packLong;
import static com.foundationdb.server.store.MemoryStore.unpackLong;

public class MemoryTableStatusCache implements TableStatusCache
{
    private final Map<Integer,VirtualTableStatus> virtualTableStatusMap = new HashMap<>();
    private final MemoryTransactionService txnService;
    private final byte[] statusPrefix;

    public MemoryTableStatusCache(MemoryTransactionService txnService, byte[] statusPrefix) {
        this.txnService = txnService;
        this.statusPrefix = statusPrefix;
    }

    public byte[] getStatusPrefix() {
        return statusPrefix;
    }

    //
    // TableStatusCache
    //

    @Override
    public TableStatus createTableStatus(Table table) {
        return new MemoryTableStatus(table);
    }

    @Override
    public synchronized TableStatus getOrCreateVirtualTableStatus(int tableID, VirtualScanFactory factory) {
        VirtualTableStatus status = virtualTableStatusMap.get(tableID);
        if(status == null) {
            status = new VirtualTableStatus(tableID, factory);
            virtualTableStatusMap.put(tableID, status);
        }
        return status;
    }

    @Override
    public void clearTableStatus(Session session, Table table) {
        TableStatus status = table.tableStatus();
        if(status instanceof VirtualTableStatus) {
            synchronized(virtualTableStatusMap) {
                virtualTableStatusMap.remove(table.getTableId());
            }
        } else {
            MemoryTableStatus tmStatus = (MemoryTableStatus)status;
            MemoryTransaction txn = txnService.getTransaction(session);
            txn.clear(tmStatus.statusKey);
        }
    }


    //
    // Internal
    //

    private class MemoryTableStatus implements TableStatus
    {
        private final int tableID;
        private final byte[] statusKey;

        private MemoryTableStatus(Table table) {
            this.tableID = table.getTableId();
            // packLong(tableID) seems like a good option but ALTER keeps same ID => doubles row count
            PrimaryKey pk = table.getPrimaryKeyIncludingInternal();
            assert pk != null : table;
            MemoryStorageDescription sd = (MemoryStorageDescription)pk.getIndex().getStorageDescription();
            this.statusKey = join(statusPrefix, sd.getUUIDBytes());
        }

        @Override
        public void rowDeleted(Session session) {
            rowsWritten(session, -1);
        }

        @Override
        public void rowsWritten(Session session, long count) {
            setRowCount(session, getRowCount(session) + count);
        }

        @Override
        public void truncate(Session session) {
            setRowCount(session, 0);
        }

        @Override
        public long getRowCount(Session session) {
            MemoryTransaction txn = txnService.getTransaction(session);
            byte[] value = txn.get(statusKey);
            return (value == null) ? 0 : unpackLong(value);
        }

        @Override
        public long getApproximateRowCount(Session session) {
            MemoryTransaction txn = txnService.getTransaction(session);
            byte[] value = txn.getUncommitted(statusKey);
            return (value == null) ? 0 : unpackLong(value);
        }

        @Override
        public int getTableID() {
            return tableID;
        }

        private void setRowCount(Session session, long rowCount) {
            MemoryTransaction txn = txnService.getTransaction(session);
            txn.set(statusKey, packLong(rowCount));
        }
    }
}
