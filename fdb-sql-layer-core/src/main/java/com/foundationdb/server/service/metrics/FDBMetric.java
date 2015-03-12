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
package com.foundationdb.server.service.metrics;

import com.foundationdb.Transaction;
import com.foundationdb.async.Future;

import java.util.List;

/** Additional methods for metrics stored in FDB. */
public interface FDBMetric<T> extends BaseMetric<T>
{
    /** Set whether this metric is enabled for this and future sessions. */
    public void setEnabled(boolean enabled);

    /** Simple <code>struct</code> for time / value pair.
     * @see #readAllValues.
     */
    public static class Value<T> {
        final long time;
        final T value;

        public Value(long time, T value) {
            this.time = time;
            this.value = value;
        }

        @Override
        public String toString() {
            return value + " at " + time;
        }
    }

    /** Retrieve all previously stored values for this metrics. */
    public Future<List<Value<T>>> readAllValues(Transaction tr);
}
