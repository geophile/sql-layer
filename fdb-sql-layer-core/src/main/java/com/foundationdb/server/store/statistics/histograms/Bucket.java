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
package com.foundationdb.server.store.statistics.histograms;

import java.util.Objects;

public final class Bucket<T> {

    // Bucket interface

    public T value() {
        return value;
    }

    public long getEqualsCount() {
        return equalsCount;
    }

    public long getLessThanCount() {
        return ltCount;
    }

    public long getLessThanDistinctsCount() {
        return ltDistinctCount;
    }

    public void addEquals() {
        ++equalsCount;
    }

    public void addLessThans(long value) {
        ltCount += value;
    }

    public void addLessThanDistincts(long value) {
        ltDistinctCount += value;
    }

    public void markMinKeyBucket() {
        minKeyBucket = true;
    }

    public boolean isMinKeyBucket() {
        return minKeyBucket;
    }

    // for use in this class

    void init(T value, int count) {
        this.value = value;
        this.equalsCount = count;
        this.ltCount = 0;
        this.ltDistinctCount = 0;
    }

    Bucket() {
    }

    // object interface

    @Override
    public String toString() {
        return String.format("<%s: %d,%d,%d>", value, equalsCount, ltCount, ltDistinctCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Bucket bucket = (Bucket) o;

        return equalsCount == bucket.equalsCount
                && ltCount == bucket.ltCount
                && ltDistinctCount == bucket.ltDistinctCount
                && Objects.deepEquals(value, bucket.value);

    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (int) (equalsCount ^ (equalsCount >>> 32));
        result = 31 * result + (int) (ltCount ^ (ltCount >>> 32));
        result = 31 * result + (int) (ltDistinctCount ^ (ltDistinctCount >>> 32));
        return result;
    }

    private T value;
    private long equalsCount;
    private long ltCount;
    private long ltDistinctCount;
    private boolean minKeyBucket = false;
}
