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

/**
 * A stream of {@link QueryBindings}.
 * 
 * The QueryBindingsCursor has three (implied) states: 
 * 
 * - CLOSED : when created, or after closeBindings() is called
 * - PENDING: after openBindings() is called, or nextBindings() is called
 * - EXHAUSTED: after nextBindings() is called and no new bindings are available
 * 
 * cancelBindings() may be called on either a PENDING or EXHAUSTED cursor
 *  - It skips forward in the set of bindings to the calling point. 
 *  - It leaves the cursor in an unknown state, you must call nextBindings() 
 *    to get the next set of bindings. 
 * 
 * In the cases where this cursor is interleaved with a @see RowCursor, 
 * the asserted assumption is calling nextBindings() or cancelBindings() 
 * must be done only with the associated cursor in a CLOSED state. 
 * 
 * The order of calls should be: 
 * 
 * bCursor,openBindings();
 * bCursor.nextBindings();
 * rCursor.open();
 * rCursor.next();
 * rCursor.close();
 * bCursor.cancelBindings(binding)
 * bCursor.nextBindings();
 * rCursor.open();
 * rCursor.next();
 * rCursor.close();
 * bCursor.closeBindings();
 * 
 */
public interface QueryBindingsCursor
{
    /** Open stream of bindings. */
    public void openBindings();

    /** Get (and make current for <code>open</code>) the next set of bindings.
     *  returns null if no new bindings
     */
    public QueryBindings nextBindings();

    /** Close stream of bindings. */
    public void closeBindings();

    /** Advance stream past given bindings. */
    public void cancelBindings(QueryBindings bindings);
}
