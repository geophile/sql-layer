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
package com.foundationdb.server.spatial;

import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.CursorBase;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.geophile.z.space.SpaceImpl;

// Allows cursor to be reset to the beginning, as long as next hasn't been called at least
// CACHE_SIZE times. Useful for wrapping IndexCursorUnidirectional's for use by geophile with
// CACHE_SIZE = 1. Geophile may do a random access, then probe the same key as an ancestor
// (retrieving one record), and then probe it again to prepare for sequential accesses.

public class CachingCursor implements BindingsAwareCursor
{
    // CursorBase interface

    @Override
    public void open()
    {
        input.open();
    }

    @Override
    public IndexRow next()
    {
        IndexRow next;
        if (cachePosition < cachePositionsFilled) {
            next = cachedRecord(cachePosition++);
        } else {
            next = (IndexRow) input.next();
            if (cachePosition < CACHE_SIZE) {
                recordCache[cachePosition++] = next;
                cachePositionsFilled = cachePosition;
            } else if (cachePosition == CACHE_SIZE) {
                resettable = false;
            }
        }
        return next;
    }

    public void close()
    {
        input.close();
    }

    @Override
    public boolean isIdle()
    {
        return input.isIdle();
    }

    @Override
    public boolean isActive()
    {
        return input.isActive();
    }

    @Override
    public boolean isClosed()
    {
        return input.isClosed();
    }

    @Override
    public void setIdle()
    {
        input.setIdle();
    }

    // BindingsAwareCursor interface

    @Override
    public void rebind(QueryBindings bindings)
    {
        assert input instanceof BindingsAwareCursor;
        ((BindingsAwareCursor) input).rebind(bindings);
    }

    // RowOrientedCursorBase interface

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        assert row instanceof IndexRow;
        IndexRow indexRow = (IndexRow) row;
        if (indexRow.z() != z) {
            throw new NotResettableException(indexRow.z());
        }
        if (!resettable) {
            throw new NotResettableException();
        }
        cachePosition = 0;
    }

    // CachingCursor interface

    public CachingCursor(long z, CursorBase<? extends Row> indexCursor)
    {
        // input is actually an IndexCursorUnidirectional. But this class only uses next(),
        // and declaring CursorBase greatly simplifies testing.
        this.z = z;
        this.input = indexCursor;
    }

    // For use by this class

    private IndexRow cachedRecord(int i)
    {
        return (IndexRow) recordCache[i];
    }

    // Class state

    private static final int CACHE_SIZE = 1;

    // Object state

    private final long z;
    private final CursorBase<? extends Row> input;
    private final Object[] recordCache = new Object[CACHE_SIZE];
    private int cachePosition = 0;
    private int cachePositionsFilled = 0;
    private boolean resettable = true;

    // Inner classes

    public static class NotResettableException extends RuntimeException
    {
        public NotResettableException()
        {}

        public NotResettableException(long z)
        {
            super(SpaceImpl.formatZ(z));
        }
    }
}
