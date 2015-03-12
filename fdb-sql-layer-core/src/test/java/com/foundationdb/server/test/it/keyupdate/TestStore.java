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
package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.IndexKeyVisitor;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.TreeRecordVisitor;
import com.foundationdb.server.types.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

// TestStore is Store-like. Not worth the effort to make it completely compatible.

public class TestStore
{
    // Store-ish interface. Update delegate first, so that if it throws an exception, map is left alone.

    public void writeRow(Session session, KeyUpdateRow row)
        throws Exception
    {
        realStore.writeRow(session, row, null, null);
        map.put(row.keyUpdateHKey(), row);
    }

    public void deleteRow(Session session, KeyUpdateRow row)
        throws Exception
    {
        realStore.deleteRow(session, row, false);
        map.remove(row.keyUpdateHKey());
    }

    public void updateRow(Session session, KeyUpdateRow oldRow, KeyUpdateRow newRow, ColumnSelector columnSelector)
    {
        realStore.updateRow(session,
                            oldRow,
                            newRow);
        KeyUpdateRow currentRow = map.remove(oldRow.keyUpdateHKey());
        assert currentRow != null;
        //KeyUpdateRow mergedRow = mergeRows(currentRow, newRow, columnSelector);
        map.put(newRow.keyUpdateHKey(), newRow);
    }

    public void traverse(Session session, Group group, TreeRecordVisitor testVisitor, TreeRecordVisitor realVisitor)
        throws Exception
    {
        realStore.traverse(session, group, realVisitor);
        for (Map.Entry<HKey, KeyUpdateRow> entry : map.entrySet()) {
            testVisitor.visit(entry.getKey().objectArray(), entry.getValue());
        }
    }

    public void traverse(Session session, Index index, IndexKeyVisitor visitor, long scanTimeLimit, long sleepTime)
    {
        realStore.traverse(session, index, visitor, scanTimeLimit, sleepTime);
    }

    // TestStore interface

    public KeyUpdateRow find(HKey hKey)
    {
        return map.get(hKey);
    }

    public TestStore(Store realStore)
    {
        this.realStore = realStore;
    }

    public void writeTestRow(KeyUpdateRow row)
    {
        map.put(row.keyUpdateHKey(), row);
    }

    public void deleteTestRow(KeyUpdateRow row)
    {
        map.remove(row.keyUpdateHKey());
    }

    // For use by this class

    // Object state

    private final SortedMap<HKey, KeyUpdateRow> map = new TreeMap<>();
    private final Store realStore;
}
