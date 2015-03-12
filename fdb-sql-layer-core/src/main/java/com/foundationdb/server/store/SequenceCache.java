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
package com.foundationdb.server.store;


import com.foundationdb.ais.model.Sequence;

/**
 * Sequence storage, cache lifetime:
 * - Each sequence gets a directory, prefix used to store a single k/v pair
 *   - key: Allocated directory prefix
 *   - value: Largest value allocated (i.e. considered consumed) for the sequence
 * - Each SQL Layer keeps a local cache of pre-allocated values (class below, configurable size)
 * - When a transaction needs a value it looks in the local cache
 *   - If the cache is empty or from a future timestamp, read + write of current_value+cache_size
 *     is made on the sequence k/v
 *   - A session post-commit hook is scheduled to update the layer wide cache
 *   - Further values will come out of the session cache
 * - Note:
 *   - The cost of updating the cache is amortized across cache_size many allocations
 *   - As there is a single k/v, updating the cache is currently serial
 *   - The layer wide cache update is a post-commit hook so it is possible to lose blocks if
 *     one connection sneaks in past a previous completed one. This only leads to gaps, not
 *     duplication.
 */
class SequenceCache
{
    private final long timestamp;
    private final long maxValue;
    private long value;

    public static Object cacheKey(Sequence s) {
        return s.getStorageUniqueKey();
    }

    public static SequenceCache newEmpty() {
        return new SequenceCache(Long.MAX_VALUE, 0, 1);
    }

    public static SequenceCache newLocal(long startValue, long cacheSize) {
        return new SequenceCache(Long.MAX_VALUE, startValue, startValue + cacheSize);
    }

    public static SequenceCache newGlobal(long timestamp, SequenceCache prevLocal) {
        return new SequenceCache(timestamp, prevLocal.value, prevLocal.maxValue);
    }


    private SequenceCache(long timestamp, long startValue, long maxValue) {
        this.timestamp = timestamp;
        this.value = startValue;
        this.maxValue = maxValue;
    }

    public synchronized long nextCacheValue() {
        if (++value == maxValue) {
            // ensure the next call to nextCacheValue also fails
            --value;
            return -1;
        }
        return value;
    }

    public synchronized long getCurrentValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("SequenceCache(@%s, %d, %d, %d)", Integer.toHexString(hashCode()), timestamp, value, maxValue);
    }
}
