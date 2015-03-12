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
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;
import com.persistit.Key;
import com.persistit.Value;

import java.util.ArrayList;
import java.util.List;

abstract class SorterAdapter<S,E,V> {
    
    protected SorterAdapter(SortKeyAdapter<S,E> sortKeyAdapter) {
        this.sortKeyAdapter = sortKeyAdapter;
        keyTarget = sortKeyAdapter.createTarget("sort");
    }
    
    public void init(RowType rowType, Ordering ordering, Key key, Value value, QueryContext context, QueryBindings bindings,
                     API.SortOption sortOption)
    {
        
        this.keyTarget.attach(key);
        
        int rowFields  = rowType.nFields();
        this.tFieldTypes = tinstances(rowFields);
        for (int i = 0; i < rowFields; i++) {
            initTypes(rowType, tFieldTypes, i);
        }
        
        attachValueTarget(value);

        preserveDuplicates = sortOption == API.SortOption.PRESERVE_DUPLICATES;
        if (preserveDuplicates) {
            // Append a count field as a sort key, to ensure key uniqueness for Persisit. By setting
            // the ascending flag equal to that of some other sort field, we don't change an all-ASC or all-DESC sort
            // into a less efficient mixed-mode sort.
            appendDummy(ordering);
        }
        
        int nsort = ordering.sortColumns();
        this.evaluations = new ArrayList<>(nsort);
        this.tOrderingTypes = tinstances(nsort);
        for (int i = 0; i < nsort; i++) {
            initTypes(ordering, i, tOrderingTypes);
            V evaluation = evaluation(ordering, context, bindings, i);
            evaluations.add(evaluation);
        }
    }

    protected abstract void appendDummy(Ordering ordering);

    protected abstract TInstance[] tinstances(int size);

    public void evaluateToKey(Row row, int i) {
        V evaluation = evaluations.get(i);
        S keySource = evaluateRow(evaluation, row);
        keyTarget.append(keySource, i, tOrderingTypes);
    }

    public TInstance[] tFieldTypes() {
        return tFieldTypes;
    }

    public boolean preserveDuplicates() {
        return preserveDuplicates;
    }
    
    protected abstract void initTypes(RowType rowType, TInstance[] tFieldTypes, int i);
    protected abstract void initTypes(Ordering ordering, int i, TInstance[] tInstances);
    protected abstract V evaluation(Ordering ordering, QueryContext context, QueryBindings bindings, int i);
    protected abstract S evaluateRow(V evaluation, Row row);
    protected abstract void attachValueTarget(Value value);

    protected abstract PersistitValueSourceAdapter createValueAdapter();
    private final SortKeyAdapter<S,E> sortKeyAdapter;

    private final SortKeyTarget<S> keyTarget;
    private boolean preserveDuplicates;
    //private AkCollator orderingCollators[];
    private TInstance tFieldTypes[], tOrderingTypes[];

    private List<V> evaluations;

    public void evaluateToTarget(Row row, int i) {
        S field = sortKeyAdapter.get(row, i);
        putFieldToTarget(field, i, tFieldTypes);
    }

    protected abstract void putFieldToTarget(S value, int i, TInstance[] tFieldTypes);

    public interface PersistitValueSourceAdapter {
        void attach(Value value);
        void putToHolders(ValuesHolderRow row, int i, TInstance[] fieldTypes);
    }
}
