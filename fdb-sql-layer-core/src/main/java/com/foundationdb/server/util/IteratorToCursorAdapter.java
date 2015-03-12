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
package com.foundationdb.server.util;

import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;

import java.util.Iterator;

public class IteratorToCursorAdapter implements BindingsAwareCursor
{
    // CursorBase interface

    @Override
    public void open()
    {
        state = CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public Row next()
    {
        Row next;
        if (iterator.hasNext()) {
            next = (Row) iterator.next();
        } else {
            next = null;
            state = CursorLifecycle.CursorState.IDLE;
        }
        return next;
    }

    @Override
    public void close()
    {
        state = CursorLifecycle.CursorState.CLOSED;
    }

    @Override
    public boolean isIdle()
    {
        return state == CursorLifecycle.CursorState.IDLE;
    }

    @Override
    public boolean isActive()
    {
        return state == CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public boolean isClosed()
    {
        return state == CursorLifecycle.CursorState.CLOSED;
    }

    @Override
    public void setIdle()
    {
        state = CursorLifecycle.CursorState.IDLE;
    }

    // RowOrientedCursorBase interface

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        throw new UnsupportedOperationException();
    }

    // BindingsAwareCursor interface

    @Override
    public void rebind(QueryBindings bindings)
    {
        throw new UnsupportedOperationException();
    }

    // IteratorToCursorAdapter interface

    public IteratorToCursorAdapter(Iterator iterator)
    {
        this.iterator = iterator;
    }

    // Object state

    private final Iterator iterator;
    private CursorLifecycle.CursorState state = CursorLifecycle.CursorState.CLOSED;
}
