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
package com.foundationdb.qp.util;

import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiCursor extends RowCursorImpl implements BindingsAwareCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
        sealed = true;
        if (openAll) {
            for (RowCursor cursor : cursors) {
                cursor.open();
            }
        }
        cursorIterator = cursors.iterator();
        startNextCursor();
    }

    @Override
    public Row next()
    {
        Row next;
        do {
            next = current != null ? current.next() : null;
            if (next == null) {
                startNextCursor();
            }
        } while (next == null && current != null);
        return next;
    }


    @Override
    public void close()
    {
        try {
            if (current != null) {
                current.close();
            }
            if (openAll) {
                while (cursorIterator.hasNext()) {
                    current = cursorIterator.next();
                    current.close();
                }
            }
        } finally {
            current = null;
            super.close();
        }
    }

    @Override
    public void rebind(QueryBindings bindings)
    {
        for (RowCursor cursor : cursors) {
            if (cursor instanceof BindingsAwareCursor) {
                ((BindingsAwareCursor)cursor).rebind(bindings);
            }
        }
    }

    // MultiCursor interface

    public MultiCursor() {
        this(false);
    }

    public MultiCursor(boolean openAll) {
        this.openAll = openAll;
    }

    public void addCursor(RowCursor cursor)
    {
        if (sealed) {
            throw new IllegalStateException();
        }
        cursors.add(cursor);
    }

    // For use by this class

    private void startNextCursor()
    {
        if (cursorIterator.hasNext()) {
            // closing last cursor
            if (current != null) {
                try {
                    current.close();
                } finally {
                    current = null;
                }
            }
            current = cursorIterator.next();
            if (!openAll) {
                current.open();
            }
        } else {
            current = null;
            setIdle();
        }
    }

    // Object state

    private final List<RowCursor> cursors = new ArrayList<>();
    private final boolean openAll;
    private boolean sealed = false;
    private Iterator<RowCursor> cursorIterator;
    private RowCursor current;

}
