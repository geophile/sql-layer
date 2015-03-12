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
package com.foundationdb.qp.loadableplan;

import com.foundationdb.qp.operator.CursorBase;
import java.util.List;

/** A cursor that returns column values directly.
 * Return columns from <code>next</code>. If an empty list is
 * returned, any buffered rows will be flushed and <code>next</code>
 * will be called again. If <code>null</code> is returned, the cursor
 * is exhausted and will be closed.
 */
public abstract class DirectObjectCursor implements CursorBase<List<?>>
{
    // These cursors are used outside of execution plans. These methods should not be called.

    @Override
    public void close()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean isIdle()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean isActive()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean isClosed()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }
    @Override
    public void setIdle()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
