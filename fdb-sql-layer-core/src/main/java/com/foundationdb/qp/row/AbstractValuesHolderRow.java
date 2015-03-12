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
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.value.ValueTargets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

class AbstractValuesHolderRow extends AbstractRow {

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueSource uncheckedValue(int i) {
        Value value = values.get(i);
        if (!value.hasAnyValue())
            throw new IllegalStateException("value at index " + i + " was never set");
        return value;
    }

    // for use by subclasses

    AbstractValuesHolderRow(RowType rowType, boolean isMutable) {
        this.isMutable = isMutable;
        this.rowType = rowType;
        int nfields = rowType.nFields();
        values = new ArrayList<>(nfields);
        for (int i = 0; i < nfields; ++i) {
            TInstance type = rowType.typeAt(i);
            values.add(new Value(type));
        }
    }

    AbstractValuesHolderRow(RowType rowType, Object... objects) {
        this.isMutable = false;
        this.rowType = rowType;
        assert rowType.nFields() == objects.length;
        
        List<Value> valueList = new ArrayList<>(rowType.nFields());
        for (int i = 0; i < objects.length; i++) {
            valueList.add(ValueSources.valuefromObject(objects[i], rowType.typeAt(i)));
        }
       
        this.values = Collections.unmodifiableList(valueList);
    }

    AbstractValuesHolderRow(RowType rowType, Value... values) {
        this (rowType, Arrays.asList(values));
    }
   
    AbstractValuesHolderRow(RowType rowType, List<Value> values) {
        this.isMutable = false;
        this.rowType = rowType;
        this.values = Collections.unmodifiableList(values);
        assert rowType.nFields() == values.size();
        for (int i = 0, max = values.size(); i < max; ++i) {
            TClass requiredType = rowType.typeAt(i).typeClass();
            TClass actualType = TInstance.tClass(values.get(i).getType());
            if (requiredType != actualType)
                throw new IllegalArgumentException("value " + i + " should be " + requiredType
                        + " but was " + actualType);
        }
    }

    AbstractValuesHolderRow(RowType rowType, boolean isMutable,
                            Iterator<? extends ValueSource> initialValues)
    {
        this(rowType, isMutable);
        int i = 0;
        while(initialValues.hasNext()) {
            if (i >= values.size())
                throw new IllegalArgumentException("too many initial values: reached limit of " + values.size());
            ValueSource nextValue = initialValues.next();
            TInstance nextValueType = nextValue.getType();
            TInstance expectedTInst = rowType.typeAt(i);
            if (TInstance.tClass(nextValueType) != TInstance.tClass(expectedTInst))
                throw new IllegalArgumentException(
                        "value at index " + i + " expected type " + expectedTInst
                                + ", but UnderlyingType was " + nextValueType + ": " + nextValue);
            ValueTargets.copyFrom(nextValue, values.get(i++));
        }
        if (i != values.size())
            throw new IllegalArgumentException("not enough initial values: required " + values.size() + " but saw " + i);
    }

    void clear() {
        checkMutable();
    }

    Value valueAt(int index) {
        checkMutable();
        return values.get(index);
    }

    private void checkMutable() {
        if (!isMutable)
            throw new IllegalStateException("can't invoke method on an immutable AbstractValuesHolderRow");
    }

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }

    private final RowType rowType;
    protected final List<Value> values;
    private final boolean isMutable;
}
