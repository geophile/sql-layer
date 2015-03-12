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

import java.util.ArrayDeque;
import java.util.Queue;

/** An {@link OperatorCursor} that opens a single {@link BindingsAwareCursor}
* for each {@link QueryBindings} with lookahead.
*/
public abstract class LookaheadLeafCursor<C extends BindingsAwareCursor> extends OperatorCursor
{
    // Cursor interface

    @Override
    public void open() {
        super.open();
        if (currentCursor != null) {
            currentCursor.open();
        }
        else if (pendingCursor != null) {
            currentCursor = pendingCursor;
            pendingCursor = null;
        }
        else {
            // At the very beginning, the pipeline isn't started.
            currentCursor = openACursor(currentBindings, false);
        }
        while (!cursorPool.isEmpty() && !bindingsExhausted) {
            QueryBindings bindings = bindingsCursor.nextBindings();
            if (bindings == null) {
                bindingsExhausted = true;
                break;
            }
            C cursor = null;
            if (bindings.getDepth() == currentBindings.getDepth()) {
                cursor = openACursor(bindings, true);
            }
            pendingBindings.add(new BindingsAndCursor<C>(bindings, cursor));
        }
    }

    @Override
    public Row next() {
        if (CURSOR_LIFECYCLE_ENABLED) {
            CursorLifecycle.checkIdleOrActive(this);
        }
        checkQueryCancelation();
        Row row = currentCursor.next();
        if (row == null) {
            currentCursor.setIdle();
        }
        return row;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector) {
        if (CURSOR_LIFECYCLE_ENABLED) {
            CursorLifecycle.checkIdleOrActive(this);
        }
        currentCursor.jump(row, columnSelector);
        state = CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public void close() {
        try {
            resetActiveCursors();
        } finally {
            super.close();
        }
    }

    @Override
    public boolean isIdle() {
        return currentCursor == null ? super.isIdle() : currentCursor.isIdle();
    }

    @Override
    public boolean isActive() {
        return currentCursor == null ? super.isActive() : currentCursor.isActive();
    }
    
    @Override
    public boolean isClosed() {
        return currentCursor == null ? super.isClosed() : currentCursor.isClosed();
    }

    @Override
    public void openBindings() {
        recyclePending();
        bindingsCursor.openBindings();
        bindingsExhausted = false;
        currentCursor = pendingCursor = null;
    }

    @Override
    public QueryBindings nextBindings() {
        CursorLifecycle.checkClosed(this);
        resetActiveCursors();
        BindingsAndCursor<C> bandc = pendingBindings.poll();
        if (bandc != null) {
            currentBindings = bandc.bindings;
            pendingCursor = bandc.cursor;
        } else {
            currentBindings = bindingsCursor.nextBindings();
            if (currentBindings == null) {
                bindingsExhausted = true;
            }
        }
        return currentBindings;
    }

    @Override
    public void closeBindings() {
        bindingsCursor.closeBindings();
        recyclePending();
    }

    @Override
    public void cancelBindings(QueryBindings bindings) {
        CursorLifecycle.checkClosed(this);
        
        if ((currentBindings != null) && currentBindings.isAncestor(bindings)) {
            resetActiveCursors();
            currentBindings = null;
        }
        while (true) {
            BindingsAndCursor<C> bandc = pendingBindings.peek();
            if (bandc == null) break;
            if (!bandc.bindings.isAncestor(bindings)) break;
            bandc = pendingBindings.remove();
            if (bandc.cursor != null) {
                try {
                    bandc.cursor.close();
                    cursorPool.add(bandc.cursor);
                } finally {
                    bandc.cursor = null;
                }
            }
        }
        bindingsCursor.cancelBindings(bindings);
    }

    // LookaheadLeafCursor interface

    LookaheadLeafCursor(QueryContext context, QueryBindingsCursor bindingsCursor, 
                        StoreAdapter adapter, int quantum) {
        super(context);
        this.bindingsCursor = bindingsCursor;
        this.pendingBindings = new ArrayDeque<>(quantum+1);
        this.cursorPool = new ArrayDeque<>(quantum);
        for (int i = 0; i < quantum; i++) {
            C cursor = newCursor(context, adapter);
            cursorPool.add(cursor);
        }
    }

    // Implemented by subclass

    protected abstract C newCursor(QueryContext context, StoreAdapter adapter);

    // Inner classes

    protected static final class BindingsAndCursor<C extends BindingsAwareCursor> {
        QueryBindings bindings;
        C cursor;

        /**
         * @param cursor An Open cursor
         */
        BindingsAndCursor(QueryBindings bindings, C cursor) {
            this.bindings = bindings;
            this.cursor = cursor;
        }
    }

    // For use by this class

    protected void resetActiveCursors() {
        if (currentCursor != null) {
            try {
                currentCursor.close();
                cursorPool.add(currentCursor);
            } finally {
                currentCursor = null;
            }
        }
        if (pendingCursor != null) {
            try {
                pendingCursor.close();
                cursorPool.add(pendingCursor);
            } finally {
                pendingCursor = null;
            }
        }
    }
    
    protected void recyclePending() {
        while (true) {
            BindingsAndCursor<C> bandc = pendingBindings.poll();
            if (bandc == null) break;
            if (bandc.cursor != null) {
                try {
                    bandc.cursor.close();
                    cursorPool.add(bandc.cursor);
                } finally {
                    bandc.cursor = null;
                }
            }
        }
    }

    protected C openACursor(QueryBindings bindings, boolean lookahead) {
        C cursor = cursorPool.remove();
        cursor.rebind(bindings);
        cursor.open();
        return cursor;
    }

    // Object state

    protected final QueryBindingsCursor bindingsCursor;
    protected final Queue<BindingsAndCursor<C>> pendingBindings;
    protected final Queue<C> cursorPool;
    protected QueryBindings currentBindings;
    protected C pendingCursor, currentCursor;
    protected boolean bindingsExhausted;// destroyed;
}
