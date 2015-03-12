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
package com.foundationdb.ais.model;

/**
 * IndexToHKey is an interface useful in constructing HKey values from an index row.
 * There are two types of entries, ordinal values and index fields. An ordinal identifies
 * a user table. An index field selects a field within the index row.
 */
public class IndexToHKey
{
    public IndexToHKey(int[] ordinals, int[] indexRowPositions)
    {
        if (ordinals.length != indexRowPositions.length) {
            throw new IllegalArgumentException("All arrays must be of equal length: " +
                                               ordinals.length + ", " +
                                               indexRowPositions.length);
        }
        this.ordinals = ordinals;
        this.indexRowPositions = indexRowPositions;
    }

    public boolean isOrdinal(int index)
    {
        return ordinals[index] >= 0;
    }

    public int getOrdinal(int index)
    {
        return ordinals[index];
    }

    public int getIndexRowPosition(int index)
    {
        return indexRowPositions[index];
    }

    public int getLength()
    {
        return ordinals.length;
    }

    // If set, value >= 0, the ith field of the hkey is this ordinal
    private final int[] ordinals;
    // If set, value >= 0, the ith field of the hkey is at this position in the index row
    private final int[] indexRowPositions;
}
