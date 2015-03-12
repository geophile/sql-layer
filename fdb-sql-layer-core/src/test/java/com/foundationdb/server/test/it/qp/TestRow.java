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
package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;

public class TestRow extends AbstractRow
{
    // Row interface

    @Override
    public RowType rowType()
    {
        return valueRow.rowType();
    }

     @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

     @Override
     public boolean isBindingsSensitive() {
         return false;
     }

     @Override
     public ValueSource uncheckedValue(int i) {
         return valueRow.value(i);
     }

    // KeyUpdateRow interface

    public TestRow(RowType rowType, Object[] fields, String hKeyString)
    {
        this(rowType, new ValuesHolderRow(rowType, fields), hKeyString);
    }

    public TestRow(RowType rowType, Object... fields) {
        this(rowType, fields, null);
    }

    public TestRow(RowType rowType, ValuesHolderRow valueRow, String hKeyString) {
        this.valueRow = valueRow;
        this.hKeyString = hKeyString;
    }

    public String persistityString() {
        return hKeyString;
    }


    // Object state

    private final ValuesHolderRow valueRow;
    private final String hKeyString;
}
