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
package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.*;
import com.foundationdb.qp.storeadapter.indexcursor.IndexCursor;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.server.api.dml.ColumnSelector;

/** Wraps an {@link IndexCursor}, providing {@link #jump} and {@link IndexScanSelector} support. */
class StoreAdapterIndexCursor extends RowCursorImpl implements BindingsAwareCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
        indexCursor.open(); // Does iterationHelper.openIteration, where iterationHelper = rowState
    }

    @Override
    public Row next()
    {
        IndexRow next;
        CursorLifecycle.checkIdleOrActive(this);
        boolean needAnother;
        do {
            if ((next = (IndexRow) indexCursor.next()) != null) {
                needAnother = !(isTableIndex ||
                                selector.matchesAll() ||
                                !next.keyEmpty() && selector.matches(next.tableBitmap()));
            } else {
                setIdle();
                needAnother = false;
            }
        } while (needAnother);
        assert (next == null) == isIdle() : "next: " + next + " vs idle " + isIdle();
        return next;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        CursorLifecycle.checkIdleOrActive(this);
        Index index = indexRowType.index();
        assert !index.isSpatial(); // Jump not yet supported for spatial indexes
        // TODO: Couldn't IndexCursor handle this?
        rowState.openIteration();
        indexCursor.jump(row, columnSelector);
        state = CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public void close()
    {
        try {
            indexCursor.close(); // IndexCursor.close() closes the rowState (IndexCursor.iterationHelper)
        } finally {
            super.close();
        }
    }

    @Override
    public void rebind(QueryBindings bindings)
    {
        indexCursor.rebind(bindings);
    }

    // For use by this package

    StoreAdapterIndexCursor(QueryContext context,
                            IndexRowType indexRowType,
                            IndexKeyRange keyRange,
                            API.Ordering ordering,
                            IndexScanSelector selector,
                            boolean openAllSubCursors)
    {
        this.indexRowType = indexRowType;
        this.isTableIndex = indexRowType.index().isTableIndex();
        this.selector = selector;
        this.rowState = context.getStore().createIterationHelper(indexRowType);
        this.indexCursor = IndexCursor.create(context, keyRange, ordering, rowState,  openAllSubCursors);
    }

    // For use by this class

    // Object state

    private final IndexRowType indexRowType;
    private final boolean isTableIndex;
    private final IterationHelper rowState;
    private IndexCursor indexCursor;
    private final IndexScanSelector selector;
}
