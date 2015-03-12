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

import java.util.ArrayList;
import java.util.List;

/**
 * A generator of {@link QueryBindingsCursor}s from a common source.
 */
public class MultipleQueryBindingsCursor implements QueryBindingsCursor
{
    private final QueryBindingsCursor input;
    private final List<QueryBindings> buffer = new ArrayList<QueryBindings>();
    private final List<SubCursor> cursors = new ArrayList<SubCursor>();
    private boolean exhausted;
    private int offset;
    
    public MultipleQueryBindingsCursor(QueryBindingsCursor input) {
        this.input = input;
        newCursor();
    }

    @Override
    public void openBindings() {
        input.openBindings();
        exhausted = false;
        buffer.clear();
        offset = 0;
        cursors.get(0).openBindings();
        for (int i = 1; i < cursors.size(); i++) {
            cursors.get(i).closeBindings();
        }
    }

    @Override
    public QueryBindings nextBindings() {
        return cursors.get(0).nextBindings();
    }

    @Override
    public void closeBindings() {
        cursors.get(0).closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings bindings) {
        cursors.get(0).cancelBindings(bindings);
        input.cancelBindings(bindings);
    }

    public QueryBindingsCursor newCursor() {
        assert (offset == 0);
        SubCursor cursor = new SubCursor();
        cursors.add(cursor);
        return cursor;
    }

    protected void shrink() {
        int minIndex = cursors.get(0).index;
        for (int i = 1; i < cursors.size(); i++) {
            SubCursor cursor = cursors.get(i);
            if (!cursor.open) continue;
            if (minIndex > cursor.index) {
                minIndex = cursor.index;
            }
        }
        while (offset < minIndex) {
            buffer.remove(0);
            offset++;
        }
    }

    class SubCursor implements QueryBindingsCursor {
        boolean open;
        int index;

        @Override
        public void openBindings() {
            assert (offset == 0);
            open = true;
            index = 0;
        }

        @Override
        public QueryBindings nextBindings() {
            assert (open);
            assert (index >= offset);
            while (index - offset >= buffer.size()) {
                if (exhausted) {
                    return null;
                }
                else {
                    QueryBindings bindings = input.nextBindings();
                    if (bindings == null) {
                        exhausted = true;
                        return null;
                    }
                    buffer.add(bindings);
                }
            }
            QueryBindings bindings = buffer.get(index - offset);
            index++;
            shrink();
            return bindings;
        }

        @Override
        public void closeBindings() {
            open = false;
        }

        @Override
        public void cancelBindings(QueryBindings ancestor) {
            while (index - offset < buffer.size()) {
                QueryBindings bindings = buffer.get(index - offset);
                if (bindings.isAncestor(ancestor)) {
                    index++;
                }
                else {
                    break;
                }
            }
            shrink();
        }
    }
}
