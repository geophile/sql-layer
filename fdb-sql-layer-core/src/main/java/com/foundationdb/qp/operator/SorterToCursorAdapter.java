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

import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.util.tap.InOutTap;

/**
 * Cursors are reusable but Sorters are not.
 * This class creates a new Sorter each time a new cursor scan is started.
 */
class SorterToCursorAdapter extends RowCursorImpl
{
    // RowCursor interface

    @Override
    public void open()
    {
        super.open();
        sorter = adapter.createSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
        cursor = sorter.sort();
        cursor.open();
        state = CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public Row next()
    {
        CursorLifecycle.checkIdleOrActive(this);
        return cursor == null ? null : cursor.next();
    }

    @Override
    public void close()
    {
        try {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
            if (sorter != null) {
                sorter.close();
                sorter = null;
            }
        } finally {
            super.close();
        }
    }

    // SorterToCursorAdapter interface

    /**
     * input must be open before calling open() on this cursor.
     * SorterToCursorAdapter does not close input, but may exhaust it.
     */
    public SorterToCursorAdapter(StoreAdapter adapter,
                                 QueryContext context,
                                 QueryBindings bindings,
                                 RowCursor input,
                                 RowType rowType,
                                 API.Ordering ordering,
                                 API.SortOption sortOption,
                                 InOutTap loadTap)
    {
        this.adapter = adapter;
        this.context = context;
        this.bindings = bindings;
        this.input = input;
        this.rowType = rowType;
        this.ordering = ordering;
        this.sortOption = sortOption;
        this.loadTap = loadTap;
    }

    private final StoreAdapter adapter;
    private final QueryContext context;
    private final QueryBindings bindings;
    private final RowCursor input;
    private final RowType rowType;
    private final API.Ordering ordering;
    private final API.SortOption sortOption;
    private final InOutTap loadTap;
    private Sorter sorter;
    private RowCursor cursor;
}
