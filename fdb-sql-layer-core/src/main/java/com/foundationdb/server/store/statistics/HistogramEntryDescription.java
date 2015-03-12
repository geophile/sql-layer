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
package com.foundationdb.server.store.statistics;

public class HistogramEntryDescription
{

    protected String keyString;
    protected long equalCount;
    protected long lessCount;
    protected long distinctCount;

    public HistogramEntryDescription(String keyString, long equalCount, long lessCount, long distinctCount) {
        this.distinctCount = distinctCount;
        this.equalCount = equalCount;
        this.keyString = keyString;
        this.lessCount = lessCount;
    }

    /** A user-visible form of the key for this entry. */
    public String getKeyString() {
        return keyString;
    }

    /** The number of samples that were equal to the key value. */
    public long getEqualCount() {
        return equalCount;
    }

    /** The number of samples that were less than the key value
     * (and greater than the previous entry's key value, if any).
     */
    public long getLessCount() {
        return lessCount;
    }

    /** The number of distinct values in the less-than range. */
    public long getDistinctCount() {
        return distinctCount;
    }

    @Override
    final public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HistogramEntryDescription)) return false;

        HistogramEntryDescription that = (HistogramEntryDescription) o;

        return distinctCount == that.distinctCount
                && equalCount == that.equalCount
                && lessCount == that.lessCount
                && keyString.equals(that.keyString);

    }

    @Override
    final public int hashCode() {
        int result = keyString.hashCode();
        result = 31 * result + (int) (equalCount ^ (equalCount >>> 32));
        result = 31 * result + (int) (lessCount ^ (lessCount >>> 32));
        result = 31 * result + (int) (distinctCount ^ (distinctCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" + getKeyString() +
                ": = " + getEqualCount() +
                ", < " + getLessCount() +
                ", distinct " + getDistinctCount() +
                "}";
    }
}
