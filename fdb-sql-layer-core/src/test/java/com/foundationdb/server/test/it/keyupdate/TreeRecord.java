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

import com.foundationdb.qp.row.Row;

public class TreeRecord
{
    @Override
    public int hashCode()
    {
        return hKey.hashCode() ^ row.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        boolean eq = o != null && o instanceof TreeRecord;
        if (eq) {
            TreeRecord that = (TreeRecord) o;
            eq = this.hKey.equals(that.hKey) && equals(this.row, that.row);
        }
        return eq;
    }

    @Override
    public String toString()
    {
        return String.format("%s -> %s", hKey, row);
    }

    public HKey hKey()
    {
        return hKey;
    }

    public Row row()
    {
        return row;
    }

    public TreeRecord(HKey hKey, Row row)
    {
        this.hKey = hKey;
        this.row = row;
    }

    public TreeRecord(Object[] hKey, Row row)
    {
        this(new HKey(hKey), row);
    }

    private boolean equals(Row x, Row y)
    {
        return
            x == y ||
            x.equals(y);
    }

    private final HKey hKey;
    private final Row row;
}
