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

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTargets;

public final class OverlayingRow extends AbstractRow {
    private final Row underlying;
    private final RowType rowType;
    protected final Value[] pOverlays;

    public OverlayingRow(Row underlying) {
        this(underlying, underlying.rowType());
    }

    public OverlayingRow(Row underlying, RowType rowType) {
        this.underlying = underlying;
        this.rowType = rowType;
        this.pOverlays = new Value[underlying.rowType().nFields()];
    }

    public OverlayingRow overlay(int index, ValueSource object) {
        if (checkOverlay(index, object)) {
            ValueTargets.copyFrom(object, pOverlays[index]);
        }
        return this;
    }

    public OverlayingRow overlay(int index, Object object) {
        if (checkOverlay(index, object)) {
            pOverlays[index].putObject(object);
        }
        return this;
    }
    
    private boolean  checkOverlay (int index, Object object) {
        if (object == null) {
            pOverlays[index] = null;
            return false;
        } else if (pOverlays[index] == null) {
            pOverlays[index] = new Value(underlying.rowType().typeAt(index));
            return true;
        }
        return true;
    }

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource uncheckedValue(int i) {
        return pOverlays[i] == null ? underlying.value(i) : pOverlays[i];

    }

    @Override
    public HKey hKey() {
        return underlying.hKey();
    }

    @Override
    public boolean isBindingsSensitive() {
        return underlying.isBindingsSensitive();
    }
}
