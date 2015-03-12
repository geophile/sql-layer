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
package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.store.FDBScanTransactionOptions;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;

public class FDBGroupCursor extends RowCursorImpl implements GroupCursor {
    private final FDBAdapter adapter;
    private final FDBStoreData storeData;
    private final Schema schema;
    private final FDBScanTransactionOptions transactionOptions;
    private HKey hKey;
    private boolean hKeyDeep;
    private GroupScan groupScan;
    // static state
    private static final PointTap TRAVERSE_COUNT = Tap.createCount("traverse: fdb group cursor");

    public FDBGroupCursor(FDBAdapter adapter, Group group, FDBScanTransactionOptions transactionOptions) {
        this.adapter = adapter;
        this.storeData = adapter.getUnderlyingStore()
            .createStoreData(adapter.getSession(), group);
        this.schema = SchemaCache.globalSchema(group.getAIS());
        this.transactionOptions = transactionOptions;
    }

    @Override
    public void rebind(HKey hKey, boolean deep) {
        CursorLifecycle.checkClosed(this);
        this.hKey = hKey;
        this.hKeyDeep = deep;
    }

    @Override
    public void open() {
        super.open();
        if (hKey == null) {
            groupScan = new FullScan();
        }
        else if (hKeyDeep) {
            groupScan = new HKeyAndDescendantScan(hKey);
        }
        else {
            groupScan = new HKeyWithoutDescendantScan(hKey);
        }
    }

    @Override
    public FDBGroupRow next() {
        CursorLifecycle.checkIdleOrActive(this);
        boolean next = isActive();
        FDBGroupRow row = null;
        if (next) {
            groupScan.advance();
            next = isActive();
            if (next) {
                Row tempRow = adapter.getUnderlyingStore().expandGroupData(adapter.getSession(), storeData, schema);
                row = new FDBGroupRow(adapter.getKeyCreator(), tempRow, storeData.persistitKey);
            }
        }
        return row;
    }

    @Override
    public void close() {
        groupScan = null;
        super.close();
    }

    private abstract class GroupScan {
        public void advance() {
            TRAVERSE_COUNT.hit();
            if (!storeData.next()) {
                setIdle();
            }
        }
    }

    private class FullScan extends GroupScan {
        public FullScan() {
            adapter.getUnderlyingStore().groupIterator(adapter.getSession(), storeData, transactionOptions);
        }
    }

    private class HKeyAndDescendantScan extends GroupScan {
        public HKeyAndDescendantScan(HKey hKey) {
            hKey.copyTo(storeData.persistitKey.clear());
            adapter.getUnderlyingStore().groupKeyAndDescendantsIterator(adapter.getSession(), storeData, transactionOptions);
        }
    }

    private class HKeyWithoutDescendantScan extends GroupScan
    {
        boolean first = true;

        @Override
        public void advance()
        {
            if(first) {
                super.advance();
                first = false;
            } else {
                setIdle();
            }
        }

        public HKeyWithoutDescendantScan(HKey hKey)
        {
            hKey.copyTo(storeData.persistitKey.clear());
            adapter.getUnderlyingStore().groupKeyIterator(adapter.getSession(), storeData, transactionOptions);
        }
    }

}
