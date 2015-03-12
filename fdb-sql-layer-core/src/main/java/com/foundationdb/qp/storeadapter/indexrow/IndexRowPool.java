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
package com.foundationdb.qp.storeadapter.indexrow;

import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.server.util.LRUCacheMap;

import java.util.*;

public class IndexRowPool
{
    public IndexRow takeIndexRow(StoreAdapter adapter, IndexRowType indexRowType)
    {
        return adapterPool(adapter).takeIndexRow(indexRowType);
    }

    public void returnIndexRow(StoreAdapter adapter, IndexRowType indexRowType, IndexRow indexRow)
    {
        adapterPool(adapter).returnIndexRow(indexRowType, indexRow);
    }

    public IndexRowPool()
    {
    }

    // For use by this class

    private AdapterPool adapterPool(StoreAdapter adapter)
    {
        LRUCacheMap<Long, AdapterPool> adapterPool = threadAdapterPools.get();
        AdapterPool pool = adapterPool.get(adapter.id());
        if (pool == null) {
            pool = new AdapterPool(adapter);
            adapterPool.put(adapter.id(), pool);
        }
        return pool;
    }


    // Class state

    private static final int CAPACITY_PER_THREAD = 10;

    // Object state

    private final ThreadLocal<LRUCacheMap<Long, AdapterPool>> threadAdapterPools =
        new ThreadLocal<LRUCacheMap<Long, AdapterPool>>()
        {
            @Override
            protected LRUCacheMap<Long, AdapterPool> initialValue()
            {
                return new LRUCacheMap<>(CAPACITY_PER_THREAD);
            }
        };

    // Inner classes

    private static class AdapterPool
    {
        public IndexRow takeIndexRow(IndexRowType indexRowType)
        {
            IndexRow indexRow = null;
            Deque<IndexRow> indexRows = indexRowCache.get(indexRowType);
            if (indexRows != null && !indexRows.isEmpty()) {
                indexRow = indexRows.removeLast();
                indexRow.reset();
            }
            if (indexRow == null) {
                indexRow = adapter.newIndexRow(indexRowType);
            }
            return indexRow;
        }

        public void returnIndexRow(IndexRowType indexRowType, IndexRow indexRow)
        {
            Deque<IndexRow> indexRows = indexRowCache.get(indexRowType);
            if (indexRows == null) {
                indexRows = new ArrayDeque<>();
                indexRowCache.put(indexRowType, indexRows);
            }
            assert !indexRows.contains(indexRow);
            indexRows.addLast(indexRow);
        }

        public AdapterPool(StoreAdapter adapter)
        {
            this.adapter = adapter;
        }

        private final StoreAdapter adapter;
        private final Map<IndexRowType, Deque<IndexRow>> indexRowCache = new HashMap<>();
    }
}
