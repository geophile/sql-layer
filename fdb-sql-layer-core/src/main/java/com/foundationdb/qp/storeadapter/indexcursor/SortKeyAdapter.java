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
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.Comparison;

public abstract class SortKeyAdapter<S, E> {
    public abstract TInstance[] createTInstances(int size);
    public abstract void setColumnMetadata(Column column, int f, TInstance[] tInstances);

    public abstract void checkConstraints(ValueRecord loExpressions,
                                          ValueRecord hiExpressions,
                                          int f,
                                          AkCollator[] collators,
                                          TInstance[] tInstances);

    public abstract S[] createSourceArray(int size);

    public abstract S get(ValueRecord valueRecord, int f);
    public abstract SortKeyTarget<S> createTarget(Object descForError);

    public abstract SortKeySource<S> createSource(TInstance type);
    public abstract long compare(TInstance type, S one, S two);
    public abstract E createComparison(TInstance type, S one, Comparison comparison, S two);
    public abstract boolean evaluateComparison(E comparison, QueryContext queryContext);
    public boolean areEqual(TInstance type, S one, S two, QueryContext queryContext) {
        E expr = createComparison(type, one, Comparison.EQ, two);
        return evaluateComparison(expr, queryContext);
    }

    public abstract boolean isNull(S source);

    public abstract S eval(Row row, int field);

    public abstract void setOrderingMetadata(Ordering ordering, int index,
                                             TInstance[] tInstances);
}
