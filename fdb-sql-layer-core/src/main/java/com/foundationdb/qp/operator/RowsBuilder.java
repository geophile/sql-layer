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
package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.ValuesRowType;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.Strings;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class RowsBuilder {

    public Deque<Row> rows() {
        return rows;
    }

    public RowType rowType() {
        return rowType;
    }

    public RowsBuilder row(Object... objs) {
        ValueSource[] values = new ValueSource[objs.length];
        for (int i = 0; i < objs.length; ++i) {
            values[i] = ValueSources.valuefromObject(objs[i], tinsts[i]);
/*            
            ValueSource psource = ValueSources.fromObject(objs[i], tinsts[i]).value();
            if (!psource.getType().equalsExcludingNullable(tinsts[i])) {
                // This assumes that anything that doesn't match is a string.
                TExecutionContext context = new TExecutionContext(null,
                                                                  tinsts[i],
                                                                  null);
                Value value = new Value(tinsts[i]);
                tinsts[i].typeClass().fromObject(context, psource, value);
                psource = value;
            }
            values[i] = psource;
*/            
        }
        return row(values);
    }

    public RowsBuilder row(ValueSource... values) {
        ArgumentValidation.isEQ("values.length", values.length, tinsts.length);
        for (int i=0; i < values.length; ++i) {
            ValueSource value = values[i];
            TInstance valueType = value.getType();
            TInstance requiredType = tinsts[i];
            if (!valueType.equalsExcludingNullable(requiredType)) {
                throw new IllegalArgumentException("type at " + i + " must be " + requiredType + ", is " + valueType);
            }
        }
        InternalValuesRow row = new InternalValuesRow(rowType, Arrays.asList(values));
        rows.add(row);
        return this;
    }

    public RowsBuilder(TInstance... tinsts) {
        this.rowType = new ValuesRowType(null, COUNT.incrementAndGet(), tinsts);
        this.tinsts = tinsts;
    }

    public RowsBuilder(Schema schema, TInstance... tinsts) {
        this.rowType = schema.newValuesType(tinsts);
        this.tinsts = tinsts;
    }

    public RowsBuilder(RowType rowType) {
        this.rowType = rowType;
        tinsts = new TInstance[this.rowType.nFields()];
        for (int i=0; i < tinsts.length; ++i) {
            tinsts[i] = this.rowType.typeAt(i);
        }
    }

    // object state

    private final RowType rowType;
    private final TInstance[] tinsts;
    private final Deque<Row> rows = new ArrayDeque<>();

    // class state

    private static final AtomicInteger COUNT = new AtomicInteger();

    // nested classes

    private static class InternalValuesRow extends AbstractRow {
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
            return values.get(i);
        }

        @Override
        public String toString() {
            return Strings.join(values, ", ");
        }

        private InternalValuesRow(RowType rowType, List<? extends ValueSource> values) {
            this.rowType = rowType;
            this.values = new ArrayList<>(values);
        }

        @Override
        public boolean isBindingsSensitive() {
            return false;
        }

        private final RowType rowType;
        private final List<ValueSource> values;
    }

}
