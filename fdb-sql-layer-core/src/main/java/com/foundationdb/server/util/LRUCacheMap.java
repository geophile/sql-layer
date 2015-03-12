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
package com.foundationdb.server.util;

import java.util.LinkedHashMap;
import java.util.Map;

/** A {@link java.util.LinkedHashMap} that overrides default methods, as described in the Javadoc, for LRU behavior. */
public class LRUCacheMap<K,V> extends LinkedHashMap<K,V>
{
    // Same as HashMap
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    // For LRU behavior
    private static final boolean LINKED_ORDER = true;

    private int capacity;

    public LRUCacheMap(int capacity) {
        this(capacity, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public LRUCacheMap(int capacity, int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor, LINKED_ORDER);
        this.capacity = capacity;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public boolean removeEldestEntry(Map.Entry entry) {
        return size() > capacity;
    }
}
