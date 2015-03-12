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
package com.foundationdb.qp.virtualadapter;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;

public class VirtualGroupCursor extends RowCursorImpl implements GroupCursor {

    @Override
    public void rebind(HKey hKey, boolean deep) {
        CursorLifecycle.checkIdle(this);
    }

    @Override
    public void open() {
        super.open();
        scan = factory.getGroupScan(adapter, group);
    }

    @Override
    public Row next() {
        CursorLifecycle.checkIdleOrActive(this);
        Row row = scan.next();
        if(row == null) {
            setIdle();
        }
        return row;
    }


    @Override
    public void close() {
        super.close();
        scan.close();
        scan = null;
    }


    // Abstraction extensions

    public interface GroupScan {
        /**
         * Get the next row from the stream.
         * @return The next row or <code>null</code> if none.
         */
        public Row next();

        /**
         * Clean up any state.
         */
        public void close();
    }
    
    public VirtualGroupCursor(VirtualAdapter adapter, Group group) {
        this.adapter = adapter;
        this.factory = VirtualAdapter.getFactory(group);
        this.group = group;
        assert this.factory != null : group;
    }
    
    private final VirtualAdapter adapter;
    private final VirtualScanFactory factory;
    private GroupScan scan;
    private Group group;
}
