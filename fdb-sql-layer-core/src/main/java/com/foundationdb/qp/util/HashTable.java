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
package com.foundationdb.qp.util;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.value.ValueTargets;
import com.google.common.collect.ArrayListMultimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HashTable {
    private ArrayListMultimap<KeyWrapper, Row> hashTable = ArrayListMultimap.create();

    private RowType hashedRowType;
    private List<TComparison> tComparisons;
    private List<AkCollator> collators;
    private boolean matchNulls;

    public List<Row> getMatchingRows(Row row, List<TEvaluatableExpression> evaluatableComparisonFields, QueryBindings bindings){
        KeyWrapper key = new KeyWrapper(row, evaluatableComparisonFields, bindings);
        if (!matchNulls && key.isNull())
            return Collections.emptyList();
        return hashTable.get(key);
    }

    public void put(Row row, List<TEvaluatableExpression> evaluatableComparisonFields, QueryBindings bindings){
        KeyWrapper key = new KeyWrapper(row, evaluatableComparisonFields, bindings);
        if (matchNulls || !key.isNull()) {
            hashTable.put(key, row);
        }
    }

    public RowType getRowType() {
        return hashedRowType;
    }

    public void setRowType(RowType rowType) {
        hashedRowType = rowType;
    }

    public void setTComparisons(List<TComparison> tComparisons) {
        this.tComparisons =  tComparisons;
    }
    public void setCollators(List<AkCollator> collators) {
        this.collators =  collators;
    }
    public void setMatchNulls(boolean matchNulls) {
        this.matchNulls = matchNulls;
    }

    public class KeyWrapper implements Comparable<KeyWrapper> {
        List<ValueSource> values = new ArrayList<>();
        int hashKey = 0;
        boolean isNull;

        @Override
        public int hashCode() {
            return hashKey;
        }

        @Override
        public boolean equals(Object x) {
            if ( !(x instanceof KeyWrapper) ||  ((KeyWrapper)x).values.size() != values.size() )
                return false;
            return (compareTo((KeyWrapper)x) == 0);
        }

        @Override
        public int compareTo(KeyWrapper other) {
            for (int i = 0; i < values.size(); i++) {
                int compare;
                if (tComparisons != null && tComparisons.get(i) != null) {
                    compare = tComparisons.get(i).compare(values.get(i).getType(), values.get(i), other.values.get(i).getType(), other.values.get(i));
                }
                else {
                    compare = TClass.compare(values.get(i).getType(), values.get(i), other.values.get(i).getType(), other.values.get(i));
                }
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }

        public boolean isNull() {
            return isNull;
        }

        public KeyWrapper(Row row, List<TEvaluatableExpression> comparisonExpressions, QueryBindings bindings){
            int i = 0;
            for (TEvaluatableExpression expression : comparisonExpressions) {
                if (row != null)
                    expression.with(row);
                if (bindings != null)
                    expression.with(bindings);
                expression.evaluate();
                ValueSource columnValue = expression.resultValue();
                if (columnValue.isNull())
                    isNull = true;
                Value valueCopy = new Value(columnValue.getType());
                ValueTargets.copyFrom(columnValue, valueCopy);
                AkCollator collator = (collators != null) ? collators.get(i) : null;
                hashKey ^= ValueSources.hash(valueCopy, collator);
                values.add(valueCopy);
                i++;
            }
        }
    }
}
