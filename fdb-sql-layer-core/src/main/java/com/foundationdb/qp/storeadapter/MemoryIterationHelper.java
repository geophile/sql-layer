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

import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.server.store.MemoryStore;
import com.foundationdb.server.store.MemoryStoreData;
import com.persistit.Key;
import com.persistit.Key.Direction;
import com.persistit.Persistit;
import com.persistit.Value;

import static com.persistit.Key.Direction.GT;
import static com.persistit.Key.Direction.GTEQ;
import static com.persistit.Key.Direction.LT;
import static com.persistit.Key.Direction.LTEQ;

public class MemoryIterationHelper implements IterationHelper
{
    private final MemoryAdapter adapter;
    private final IndexRowType rowType;
    private final MemoryStoreData storeData;
    // Initialized upon traversal
    private long lastKeyGen;
    private Direction itDir;

    public MemoryIterationHelper(MemoryAdapter adapter, IndexRowType indexRowType) {
        this.adapter = adapter;
        this.rowType = indexRowType.physicalRowType();
        this.storeData = adapter.getUnderlyingStore().createStoreData(adapter.getSession(), rowType.index());
        this.storeData.persistitValue = new Value((Persistit)null);
    }

    //
    // IterationHelper
    //

    @Override
    public Key key() {
        return storeData.persistitKey;
    }

    @Override
    public Key endKey() {
        return storeData.endKey;
    }

    @Override
    public void clear() {
        storeData.persistitKey.clear();
        storeData.persistitValue.clear();
        lastKeyGen = -1;
        itDir = null;
    }

    @Override
    public void openIteration() {
        // None, iterator created on demand
    }

    @Override
    public void closeIteration() {
        //adapter.returnIndexRow(row);
    }

    @Override
    public Row row() {
        assert (storeData.rawKey != null) : "Called for chopped key (or before iterating)"; // See advanceLogical() for former
        IndexRow row = adapter.takeIndexRow(rowType);
        // updateKey() called from advance
        MemoryStore.unpackValue(storeData);
        row.copyFrom(storeData.persistitKey, storeData.persistitValue);
        return row;
    }

    @Override
    public boolean traverse(Direction dir) {
        checkIterator(dir, true);
        return advance();
    }

    @Override
    public void preload(Direction dir, boolean endInclusive) {
        checkIterator(dir, endInclusive);
    }

    //
    // Internal
    //
    /** Advance iterator with pure physical (i.e. key order) traversal. */
    private boolean advance() {
        if(storeData.next()) {
            MemoryStore.unpackKey(storeData);
            lastKeyGen = storeData.persistitKey.getGeneration();
            return true;
        }
        return false;
    }

    /** Check current iterator matches direction and recreate if not. */
    private void checkIterator(Direction dir, boolean endInclusive) {
        final boolean keyGenMatches = (lastKeyGen == storeData.persistitKey.getGeneration());
        if((itDir != dir) || !keyGenMatches) {
            // If the last key we returned hasn't changed and moving in the same direction, new iterator isn't needed.
            if(keyGenMatches) {
                if((itDir == GTEQ && dir == GT) || (itDir == LTEQ && dir == LT)) {
                    itDir = dir;
                    return;
                }
            }
            final boolean reverse = (dir == LT) || (dir == LTEQ);
            // Note: storeData.persistitKey is already adjusted appropriately for endInclusive.
            //       indexIterator() API will need to change when Key is no longer used.
            adapter.getUnderlyingStore().indexIterator(adapter.getSession(), storeData, reverse);
            lastKeyGen = storeData.persistitKey.getGeneration();
            itDir = dir;
        }
    }
}
