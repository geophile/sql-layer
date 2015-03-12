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
package com.foundationdb.server.test.it.sort;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.RowsBuilder;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.TestOperator;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertEquals;

public abstract class SorterITBase extends ITBase {
    private static final Boolean ASC = Boolean.TRUE;
    private static final Boolean DESC = Boolean.FALSE;
    private static InOutTap TEST_TAP = Tap.createTimer("test");

    private static final List<String[]> SINGLE_UNORDERED  = list("beta", "alpha", "gamma", "delta");
    private static final List<String[]> SINGLE_ASCENDING  = list("alpha", "beta", "delta", "gamma");
    private static final List<String[]> SINGLE_DESCENDING = list("gamma", "delta", "beta", "alpha");

    private static final List<String[]> MULTI_UNORDERED = list("a,b", "a,a", "b,b", "b,a");
    private static final List<String[]> MULTI_ASC_ASC   = list("a,a", "a,b", "b,a", "b,b");
    private static final List<String[]> MULTI_ASC_DESC  = list("a,b", "a,a", "b,b", "b,a");
    private static final List<String[]> MULTI_DESC_DESC = list("b,b", "b,a", "a,b", "a,a");
    private static final List<String[]> MULTI_DESC_ASC  = list("b,a", "b,b", "a,a", "a,b");


    public abstract Sorter createSorter(QueryContext context,
                                        QueryBindings bindings,
                                        Cursor input,
                                        RowType rowType,
                                        API.Ordering ordering,
                                        API.SortOption sortOption,
                                        InOutTap loadTap);


    @Test
    public void singleFieldAscending() {
        runTest(API.SortOption.PRESERVE_DUPLICATES, SINGLE_UNORDERED, SINGLE_ASCENDING, ASC);
    }

    @Test
    public void singleFieldDescending() {
        runTest(API.SortOption.PRESERVE_DUPLICATES, SINGLE_UNORDERED, SINGLE_DESCENDING, DESC);
    }

    @Test
    public void multiFieldAscAsc() {
        runTest(API.SortOption.PRESERVE_DUPLICATES, MULTI_UNORDERED, MULTI_ASC_ASC, ASC, ASC);
    }

    @Test
    public void multiFieldAscDesc() {
        runTest(API.SortOption.PRESERVE_DUPLICATES, MULTI_UNORDERED, MULTI_ASC_DESC, ASC, DESC);
    }

    @Test
    public void multiFieldDescDesc() {
        runTest(API.SortOption.PRESERVE_DUPLICATES, MULTI_UNORDERED, MULTI_DESC_DESC, DESC, DESC);
    }

    @Test
    public void multiFieldDescAsc() {
        runTest(API.SortOption.PRESERVE_DUPLICATES, MULTI_UNORDERED, MULTI_DESC_ASC, DESC, ASC);
    }

    @Test
    public void firstRowNullAscending() {
        List<String[]> input = new ArrayList<>();
        input.add(new String[]{null});
        input.addAll(SINGLE_UNORDERED);
        List<String[]> expected = new ArrayList<>();
        expected.add(new String[]{null});
        expected.addAll(SINGLE_ASCENDING);
        runTest(API.SortOption.PRESERVE_DUPLICATES, input, expected, ASC);
    }


    protected static List<String[]> list(String... values) {
        List<String[]> rows = new ArrayList<>();
        for(String s : values) {
            String[] fields = s.split(",");
            rows.add(fields);
        }
        return rows;
    }

    protected static RowsBuilder createBuilder(List<String[]> values) {
        TInstance[] tinsts = new TInstance[values.get(0).length];
        Arrays.fill(tinsts, MString.varchar());
        RowsBuilder rowsBuilder = new RowsBuilder(tinsts);
        for(String[] s : values) {
            rowsBuilder.row(s);
        }
        return rowsBuilder;
    }

    protected void runTest(API.SortOption sortOption, List<String[]> input, List<String[]> expected, boolean... fieldOrdering) {
        assertEquals("input = expected size", input.size(), expected.size());

        RowsBuilder inputRows = createBuilder(input);
        StoreAdapter adapter = newStoreAdapter();
        TestOperator inputOperator = new TestOperator(inputRows);

        QueryContext context = queryContext(adapter);
        QueryBindings bindings = context.createBindings();
        Cursor inputCursor = API.cursor(inputOperator, context, bindings);
        inputCursor.openTopLevel();

        API.Ordering ordering = API.ordering();
        for(int i = 0; i < fieldOrdering.length; ++i) {
            ordering.append(field(inputOperator.rowType(), i), fieldOrdering[i]);
        }

        Sorter sorter = createSorter(context, bindings, inputCursor, inputOperator.rowType(), ordering, sortOption, TEST_TAP);
        RowCursor sortedCursor = sorter.sort();

        Row[] expectedRows = createBuilder(expected).rows().toArray(new Row[expected.size()]);
        compareRows(expectedRows, sortedCursor);
    }
}
