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

import com.foundationdb.qp.row.Row;
import com.persistit.Key;
import com.persistit.Key.Direction;

public interface IterationHelper
{
    /** Get the (stateful) key associated with this helper. */
    Key key();
    Key endKey();

    /** Clear internal state, including the {@link #key()}. */
    void clear();

    /** Begin a new iteration. */
    void openIteration();

    /** Close the current iteration. */
    void closeIteration();

    /**
     * Get the row for that last advancement.
     * <p/>
     * <i>
     *     Note: {@link #traverse(Direction, boolean)} must be called prior to this method.
     * </i>
     */
    Row row();

    /**
     * Advance internal state.
     * @param dir The direction to advance in.
     * @return <code>true</code> if there was a key/value to traverse to.
     */
    boolean traverse(Direction dir);

    /**
     * Start cursor for given direction if that can be done asynchronously.
     * @param dir The direction to advance in.
     */
    void preload(Direction dir, boolean endInclusive);
}
