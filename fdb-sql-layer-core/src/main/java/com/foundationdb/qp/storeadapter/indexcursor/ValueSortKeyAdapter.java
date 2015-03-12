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

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.PersistitKeyValueTarget;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.server.types.texpressions.TComparisonExpression;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import com.persistit.Key;

public class ValueSortKeyAdapter extends SortKeyAdapter<ValueSource, TPreparedExpression> {
    
    private ValueSortKeyAdapter() {}
    
    public static final SortKeyAdapter<ValueSource, TPreparedExpression> INSTANCE = new ValueSortKeyAdapter();

    @Override
    public TInstance[] createTInstances(int size) {
        return new TInstance[size];
    }

    @Override
    public void setColumnMetadata(Column column, int f, TInstance[] tInstances) {
        tInstances[f] = column.getType();
    }

    @Override
    public void checkConstraints(ValueRecord loExpressions,
                                 ValueRecord hiExpressions,
                                 int f,
                                 AkCollator[] collators,
                                 TInstance[] types) {
        ValueSource loValueSource = loExpressions.value(f);
        ValueSource hiValueSource = hiExpressions.value(f);
        if (!TClass.areEqual(loValueSource, hiValueSource)) {
            throw new IllegalArgumentException(String.format("lo: %s <> hi: %s", loValueSource, hiValueSource));
        }
    }

    @Override
    public ValueSource[] createSourceArray(int size) {
        return new ValueSource[size];
    }

    @Override
    public ValueSource get(ValueRecord valueRecord, int f) {
        return valueRecord.value(f);
    }

    @Override
    public SortKeyTarget<ValueSource> createTarget(Object descForError) {
        return new ValueSortKeyTarget(descForError);
    }

    @Override
    public boolean isNull(ValueSource source) {
        return source.isNull();
    }

    @Override
    public SortKeySource<ValueSource> createSource(TInstance type) {
        return new ValueSortKeySource(type);
    }

    @Override
    public long compare(TInstance type, ValueSource one, ValueSource two) {
        return TClass.compare(type, one, type, two);
    }

    @Override
    public TPreparedExpression createComparison(TInstance type,
                                                ValueSource one,
                                                Comparison comparison,
                                                ValueSource two) {
        TPreparedExpression arg1 = new TPreparedLiteral(type, one);
        TPreparedExpression arg2 = new TPreparedLiteral(type, two);
        return new TComparisonExpression(arg1, comparison, arg2);
    }

    @Override
    public boolean evaluateComparison(TPreparedExpression comparison, QueryContext queryContext) {
        TEvaluatableExpression eval = comparison.build();
        eval.evaluate();
        return eval.resultValue().getBoolean();
    }

    @Override
    public ValueSource eval(Row row, int field) {
        return row.value(field);
    }

    @Override
    public void setOrderingMetadata(API.Ordering ordering, int index,
                                    TInstance[] tInstances) {
        tInstances[index] = ordering.type(index);
    }

    private static class ValueSortKeyTarget implements SortKeyTarget<ValueSource> {

        public ValueSortKeyTarget(Object descForError) {
            this.target = new PersistitKeyValueTarget(descForError);
        }

        @Override
        public void attach(Key key) {
            target.attach(key);
        }

        @Override
        public void append(ValueSource source, int f, TInstance[] tInstances)
        {
            append(source, tInstances[f]);
        }

        @Override
        public void append(ValueSource source, TInstance type) {
            if (source.isNull()) {
                target.putNull();
            } else {
                type.writeCollating(source, target);
            }
        }

        protected final PersistitKeyValueTarget target;
    }
    
    private static class ValueSortKeySource implements SortKeySource<ValueSource> {
        @Override
        public void attach(Key key, int i, TInstance type) {
            source.attach(key, i, type);
        }

        @Override
        public ValueSource asSource() {
            return source;
        }
        
        public ValueSortKeySource(TInstance type) {
            source = new PersistitKeyValueSource(type);
        }
        
        private final PersistitKeyValueSource source;
    }
}
