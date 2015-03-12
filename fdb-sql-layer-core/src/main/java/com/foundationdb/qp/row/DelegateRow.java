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
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;

public class DelegateRow implements Row {
    private final Row delegate;

    public DelegateRow(Row delegate) {
        this.delegate = delegate;
    }

    public Row getDelegate() {
        return delegate;
    }

    //
    // Row
    //

    @Override
    public RowType rowType() {
        return delegate.rowType();
    }

    @Override
    public HKey hKey() {
        return delegate.hKey();
    }

    @Override
    public HKey ancestorHKey(Table table) {
        return delegate.ancestorHKey(table);
    }

    @Override
    public boolean ancestorOf(Row that) {
        return delegate.ancestorOf(that);
    }

    @Override
    public boolean containsRealRowOf(Table table) {
        return delegate.containsRealRowOf(table);
    }

    @Override
    public Row subRow(RowType subRowType) {
        return delegate.subRow(subRowType);
    }

    @Override
    public int compareTo(Row row, int leftStartIndex, int rightStartIndex, int fieldCount) {
        return delegate.compareTo(row, leftStartIndex, rightStartIndex, fieldCount);
    }

    @Override
    public ValueSource value(int index) {
        return delegate.value(index);
    }

    @Override
    public boolean isBindingsSensitive() {
        return delegate.isBindingsSensitive();
    }
}
