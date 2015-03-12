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

import com.foundationdb.qp.rowtype.CompoundRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

public class CompoundRow extends AbstractRow {

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        return null;
    }

    @Override
    public ValueSource uncheckedValue(int i) {
        ValueSource source;
        if (i < firstRowFields) {
            source = firstRow == null ? nullValue(i) : firstRow.value(i);
        } else {
            source = secondRow == null ? nullValue(i) : secondRow.value(i - rowOffset);
        }
        return source;
    }
    
    @Override
    public Row subRow(RowType subRowType)
    {
        Row subRow = null;
        if (subRowType == rowType.first()) {
            subRow = firstRow;
        } else if (subRowType == rowType.second()) {
            subRow = secondRow;
        } else {
            // If the subRowType doesn't match leftType or rightType, then it might be buried deeper.
            if(firstRow != null) {
                subRow = firstRow.subRow(subRowType);
            }
            if (subRow == null && secondRow != null) {
                subRow = secondRow.subRow(subRowType);
            }
        }
        return subRow;
    }

    protected Row first() {
        return firstRow;
    }
    
    protected Row second() {
        return secondRow;
    }
    
    protected int firstRowFields() {
        return firstRowFields;
    }
    
    public CompoundRow (CompoundRowType type, Row firstRow, Row secondRow)
    {
        this.rowType = type;
        this.firstRow = firstRow;
        this.secondRow = secondRow;
        this.firstRowFields = type.first().nFields();
        this.rowOffset = type.first().nFields();
    }

    private ValueSource nullValue(int i) {
        return ValueSources.getNullSource(rowType.typeAt(i));
    }

    @Override
    public boolean isBindingsSensitive() {
        if (firstRow != null && firstRow.isBindingsSensitive()) {
            return true;
        }
        else if (secondRow != null && secondRow.isBindingsSensitive()) {
            return true;
        }
        return false;
    }

    public Row getFirstRow() {
        return firstRow;
    }

    public Row getSecondRow() {
        return secondRow;
    }

    // Object state

    private final CompoundRowType rowType;
    private final Row firstRow;
    private final Row secondRow;
    private final int firstRowFields;
    protected int rowOffset; 

}
