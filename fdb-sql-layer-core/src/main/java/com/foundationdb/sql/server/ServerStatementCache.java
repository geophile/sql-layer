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
package com.foundationdb.sql.server;

import com.foundationdb.server.util.LRUCacheMap;

/**
 * Cache of parsed statements.
 */
public class ServerStatementCache<T extends ServerStatement>
{
    private final CacheCounters counters;
    private final LRUCacheMap<String,T> cache;

    public ServerStatementCache(CacheCounters counters, int size) {
        this.counters = counters;
        this.cache = new LRUCacheMap<>(size);
    }

    public int getCapacity() {
        return cache.getCapacity();
    }

    public synchronized void setCapacity(int capacity) {
        cache.setCapacity(capacity);
        cache.clear();
    }

    public synchronized T get(String sql) {
        T entry = cache.get(sql);
        if (entry != null)
            counters.incrementHits();
        else
            counters.incrementMisses();
        return entry;
    }

    public synchronized void put(String sql, T stmt) {
        // TODO: Count number of times this is non-null, meaning that
        // two threads computed the same statement?
        cache.put(sql, stmt);
    }

    public synchronized void invalidate() {
        cache.clear();
    }

    public synchronized void reset() {
        cache.clear();
    }
}
