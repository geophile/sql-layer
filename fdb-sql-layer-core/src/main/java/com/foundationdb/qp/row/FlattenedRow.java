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
package com.foundationdb.qp.row;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.rowtype.FlattenedRowType;

public class FlattenedRow extends CompoundRow
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s, %s", first(), second());
    }

    // Row interface

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    @Override
    public boolean containsRealRowOf(Table table)
    {
        return     (first() != null && first().rowType().hasTable() && first().rowType().table() == table)
                   || (second() != null && second().rowType().hasTable() && second().rowType().table() == table)
                   || (first() != null && first().containsRealRowOf(table))
                   || (second() != null && second().containsRealRowOf(table))
            ;
    }

    // FlattenedRow interface

    public FlattenedRow(FlattenedRowType rowType, Row parent, Row child, HKey hKey)
    {
        super (rowType, parent, child);
        this.hKey = hKey;
        if (parent != null && child != null) {
            // assert parent.runId() == child.runId();
        }
        if (parent != null && !rowType.parentType().equals(parent.rowType())) {
            throw new IllegalArgumentException("mismatched type between " +rowType+ " and parent " + parent.rowType());
        }
    }

    // Object state

    private final HKey hKey;
}
