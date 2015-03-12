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

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;

/**
 * Abstract implementation of RowCursor. 
 * 
 *  Implements the state checking for the CursorBase, but not any of the
 *  operations methods. If you change this, also change @See OperatorExecutionBase
 *  as the two set of state implementations should match. 
 */
public abstract class RowCursorImpl implements RowCursor {

    @Override
    public void open() {
        CursorLifecycle.checkClosed(this);
        state = CursorLifecycle.CursorState.ACTIVE;
    }
    
    @Override 
    public void close() {
        if (ExecutionBase.CURSOR_LIFECYCLE_ENABLED) {
            CursorLifecycle.checkIdleOrActive(this);
        }
        state = CursorLifecycle.CursorState.CLOSED;
    }
    
    @Override
    public void setIdle() {
        state = CursorLifecycle.CursorState.IDLE;
    }

    // Used in IndexCursorUnidirections#jump() to reactivate the cursor
    protected void setActive() {
        CursorLifecycle.checkIdleOrActive(this);
        state = CursorLifecycle.CursorState.ACTIVE;
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
    public void jump(Row row, ColumnSelector columnSelector) {
        throw new UnsupportedOperationException();            
    }

    // Object State
    protected CursorLifecycle.CursorState state = CursorLifecycle.CursorState.CLOSED;
}
