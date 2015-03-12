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

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.virtualadapter.VirtualScanFactory;
import com.foundationdb.server.service.session.Session;

import java.util.HashMap;
import java.util.Map;

public class SimpleTableStatusCache implements TableStatusCache {
    private final Map<Integer,VirtualTableStatus> virtualStatusMap = new HashMap<>();
            
    @Override
    public synchronized TableStatus createTableStatus(Table table) {
        return new SimpleTableStatus(table.getTableId());
    }
    
    @Override
    public synchronized TableStatus getOrCreateVirtualTableStatus(int tableID, VirtualScanFactory factory) {
        VirtualTableStatus status = virtualStatusMap.get(tableID);
        if(status == null) {
            status = new VirtualTableStatus(tableID, factory);
            virtualStatusMap.put(tableID, status);
        }
        return status;
    }

    @Override
    public synchronized void clearTableStatus(Session session, Table table) {
        virtualStatusMap.remove(table.getTableId());
    }

    //
    // Internal
    //

    private static class SimpleTableStatus implements TableStatus
    {
        private final int tableID;
        private long rowCount;

        public SimpleTableStatus(int tableID) {
            this.tableID = tableID;
        }

        @Override
        public synchronized void rowDeleted(Session session) {
            --rowCount;
        }

        @Override
        public synchronized void rowsWritten(Session session, long count) {
            this.rowCount += count;
        }

        @Override
        public synchronized void truncate(Session session) {
            this.rowCount = 0;
        }

        @Override
        public synchronized long getRowCount(Session session) {
            return rowCount;
        }

        @Override
        public synchronized long getApproximateRowCount(Session session) {
            return rowCount;
        }

        @Override
        public int getTableID() {
            return tableID;
        }
    }
}
