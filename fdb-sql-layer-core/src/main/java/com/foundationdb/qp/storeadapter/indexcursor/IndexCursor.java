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

import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;
import com.persistit.Key;
import com.persistit.Key.Direction;

public abstract class IndexCursor extends RowCursorImpl implements BindingsAwareCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
        iterationHelper.openIteration();
    }

    @Override
    public Row next()
    {
        CursorLifecycle.checkIdleOrActive(this);
        return null;
    }

    @Override
    public void close()
    {
        try {
            iterationHelper.closeIteration();
        } finally {
            super.close();
        }
    }

    @Override
    public void rebind(QueryBindings bindings)
    {
        this.bindings = bindings;
    }
    
    // For use by subclasses

    protected boolean traverse(Direction dir)
    {
        return iterationHelper.traverse(dir);
    }

    protected void clear()
    {
        iterationHelper.clear();
    }

    protected Key key()
    {
        return iterationHelper.key();
    }
    
    protected Key endKey()
    {
        return iterationHelper.endKey();
    }

    // IndexCursor interface

    public static IndexCursor create(QueryContext context,
                                     IndexKeyRange keyRange,
                                     API.Ordering ordering,
                                     IterationHelper iterationHelper,
                                     boolean openAllSubCursors)
    {
        IndexCursor indexCursor;
        if (keyRange != null && keyRange.spatialCoordsIndex()) {
            if (keyRange.hi() == null) {
                indexCursor = IndexCursorSpatial_NearPoint.create(context, iterationHelper, keyRange);
            } else {
                indexCursor = IndexCursorSpatial_InBox.create(context, iterationHelper, keyRange, openAllSubCursors);
            }
        } else if (keyRange != null && keyRange.spatialObjectIndex()) {
            indexCursor = IndexCursorSpatial_InBox.create(context, iterationHelper, keyRange, openAllSubCursors);
        } else {
            SortKeyAdapter<?, ?> adapter = ValueSortKeyAdapter.INSTANCE;
            assert ordering.allAscending() || ordering.allDescending() : "Index API.ordering not all ascending/descending";
            indexCursor = IndexCursorUnidirectional.create(context, iterationHelper, keyRange, ordering, adapter);
        }
        return indexCursor;
    }

    // For use by subclasses

    protected IndexCursor(QueryContext context, IterationHelper iterationHelper)
    {
        this.context = context;
        this.adapter = context.getStore();
        this.iterationHelper = iterationHelper;
    }

    protected Row row()
    {
        return iterationHelper.row();
    }

    // Object state

    protected final QueryContext context;
    protected final StoreAdapter adapter;
    protected final IterationHelper iterationHelper;
    protected QueryBindings bindings;

    static final PointTap INDEX_TRAVERSE = Tap.createCount("traverse: index cursor");
}
