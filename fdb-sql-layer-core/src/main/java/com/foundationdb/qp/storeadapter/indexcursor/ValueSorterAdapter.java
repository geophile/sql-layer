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
package com.foundationdb.qp.storeadapter.indexcursor;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.server.PersistitValueValueSource;
import com.foundationdb.server.PersistitValueValueTarget;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TNullExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.persistit.Value;

final class ValueSorterAdapter extends SorterAdapter<ValueSource, TPreparedExpression, TEvaluatableExpression> {
    @Override
    protected void appendDummy(API.Ordering ordering) {
        ordering.append(DUMMY_EXPRESSION, ordering.ascending(0));
    }

    @Override
    protected TInstance[] tinstances(int size) {
        return new TInstance[size];
    }


    @Override
    protected void initTypes(RowType rowType, TInstance[] tFieldTypes, int i) {
        tFieldTypes[i] = rowType.typeAt(i);
    }

    @Override
    protected void initTypes(API.Ordering ordering, int i, TInstance[] tInstances) {
        tInstances[i] = ordering.type(i);
    }

    @Override
    protected TEvaluatableExpression evaluation(API.Ordering ordering, QueryContext context, QueryBindings bindings, int i) {
        TEvaluatableExpression evaluation = ordering.expression(i).build();
        evaluation.with(context);
        evaluation.with(bindings);
        return evaluation;
    }

    @Override
    protected ValueSource evaluateRow(TEvaluatableExpression evaluation, Row row) {
        evaluation.with(row);
        evaluation.evaluate();
        return evaluation.resultValue();
    }

    @Override
    protected void attachValueTarget(Value value) {
        valueTarget.attach(value);
    }

    @Override
    protected PersistitValueSourceAdapter createValueAdapter() {
        return new InternalPAdapter();
    }

    @Override
    protected void putFieldToTarget(ValueSource value, int i, TInstance[] tFieldTypes) {
        tFieldTypes[i].writeCanonical(value, valueTarget);
    }

    ValueSorterAdapter() {
        super(ValueSortKeyAdapter.INSTANCE);
    }
    
    private class InternalPAdapter implements PersistitValueSourceAdapter {

        @Override
        public void attach(Value value) {
            valueSource.attach(value);
        }

        @Override
        public void putToHolders(ValuesHolderRow row, int i, TInstance[] fieldTypes) {
            valueSource.getReady(fieldTypes[i]);
            fieldTypes[i].writeCanonical(valueSource, row.valueAt(i));
        }

        private final PersistitValueValueSource valueSource = new PersistitValueValueSource();
    }
    
    private final PersistitValueValueTarget valueTarget = new PersistitValueValueTarget();
    
    private static final TPreparedExpression DUMMY_EXPRESSION = new TNullExpression(InternalIndexTypes.LONG.instance(true));
}
