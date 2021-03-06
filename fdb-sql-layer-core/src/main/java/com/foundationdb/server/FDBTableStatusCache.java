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

import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.store.FDBStoreDataHelper;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.MutationType;
import com.foundationdb.Transaction;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.virtualadapter.VirtualScanFactory;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Directory usage:
 * <pre>
 * root_dir/
 *   tableStatus/
 * </pre>
 *
 * <p>
 *     The above directory is used to store row count information on a per-table basis.
 *     Each key is formed by pre-pending the directory prefix with the primary key's
 *     prefix and  the string "rowCount". The count is a little-endian encoded
 *     long (for {@link Transaction#mutate} usage).
 * </p>
 */
public class FDBTableStatusCache implements TableStatusCache {
    private static final List<String> TABLE_STATUS_DIR_PATH = Arrays.asList("tableStatus");
    private static final byte[] ROW_COUNT_PACKED = Tuple2.from("rowCount").pack();

    private final FDBTransactionService txnService;
    private final Map<Integer,VirtualTableStatus> virtualTableStatusMap = new HashMap<>();

    private byte[] packedTableStatusPrefix;


    public FDBTableStatusCache(FDBHolder holder, FDBTransactionService txnService) {
        this.txnService = txnService;
        this.packedTableStatusPrefix = holder.getRootDirectory().createOrOpen(holder.getTransactionContext(),
                                                                              TABLE_STATUS_DIR_PATH).get().pack();
    }

    @Override 
    public synchronized TableStatus createTableStatus(Table table) {
        return new FDBTableStatus(table);
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
    public synchronized void clearTableStatus(Session session, Table table) {
        TableStatus status = table.tableStatus();
        if(status instanceof FDBTableStatus) {
            ((FDBTableStatus)status).clearState(session);
        } else if(status != null) {
            assert status instanceof VirtualTableStatus : status;
            virtualTableStatusMap.remove(table.getTableId());
        }
    }

    public static byte[] packForAtomicOp(long l) {
        byte[] bytes = new byte[8];
        AkServerUtil.putLong(bytes, 0, l);
        return bytes;
    }

    public static long unpackForAtomicOp(byte[] bytes) {
        if(bytes == null) {
            return 0;
        }
        assert bytes.length == 8 : bytes.length;
        return AkServerUtil.getLong(bytes, 0);
    }


    /**
     * Use FDBCounter for row count and single k/v for others.
     */
    private class FDBTableStatus implements TableStatus {
        private final int tableID;
        private volatile byte[] rowCountKey;

        public FDBTableStatus(Table table) {
            this.tableID = table.getTableId();
            byte[] prefixBytes = FDBStoreDataHelper.prefixBytes(table.getPrimaryKeyIncludingInternal().getIndex());
            this.rowCountKey = ByteArrayUtil.join(packedTableStatusPrefix, prefixBytes, ROW_COUNT_PACKED);
        }

        @Override
        public void rowDeleted(Session session) {
            rowsWritten(session, -1);
        }

        @Override
        public void rowsWritten(Session session, long count) {
            TransactionState txn = txnService.getTransaction(session);
            txn.mutate(MutationType.ADD, rowCountKey, packForAtomicOp(count));
        }

        @Override
        public void truncate(Session session) {
            TransactionState txn = txnService.getTransaction(session);
            txn.setBytes(rowCountKey, packForAtomicOp(0));
        }

        @Override
        public long getRowCount(Session session) {
            return getRowCount (txnService.getTransaction(session), false);
        }

        @Override
        public long getApproximateRowCount(Session session) {
            // TODO: Snapshot avoids conflicts but might still round trip. Cache locally for some time frame?
            return getRowCount(txnService.getTransaction(session), true);
        }

        @Override
        public int getTableID() {
            return tableID;
        }

        private void clearState(Session session) {
            TransactionState txn = txnService.getTransaction(session);
            txn.clearKey(rowCountKey);
        }

        private long getRowCount(TransactionState txn, boolean snapshot) {
            if (snapshot) {
                return unpackForAtomicOp(txn.getSnapshotValue(rowCountKey));
            } else {
                return unpackForAtomicOp(txn.getValue(rowCountKey));
            }
        }
    }
}
