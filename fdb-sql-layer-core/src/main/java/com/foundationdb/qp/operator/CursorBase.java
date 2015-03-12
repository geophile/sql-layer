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

/*

A Cursor is used to scan a sequence of rows resulting from the execution of an operator.
The same cursor may be used for multiple executions of the operator, possibly with different bindings.

A Cursor is always in one of four states:

- CLOSED: The cursor is not currently involved in a scan. Invocations of next() on 
        the cursor will raise an exception. The cursor is in this state when one of
        the following is true:
        - open() has never been called.
        - The most recent method invocation was to close().

- ACTIVE: The cursor is currently involved in a scan. The cursor is in this state when one
      of the following is true:
      - The most recent method invocation was to open().
      - The most recent method invocation was to next(), which returned a non-null value.

- IDLE: The cursor is currently involved in a scan. The cursor is in this state when one
      of the following is true:
      - The most recent method invocation was to next(), which returned null.

The Cursor lifecycle is as follows:

                close                  
        +-------------------+          
        |                   |          
        v       open        +          
        CLOSED +-----> ACTIVE <-------+
        ^                   +         |
        |close      next    | next    |
        |           == null | != null |
        +-+ IDLE <----------v---------+
 */

public interface CursorBase<T>
{
    /**
     * Starts a cursor scan.
     */
    void open();

    /**
     * Advances to and returns the next object.
     * @return The next object, or <code>null</code> if at the end.
     */
    T next();

    /**
     * Terminates the current cursor scan.
     */
    void close();

    /**
     * Indicates whether the cursor is in the IDLE state.
     * @return true iff the cursor is IDLE.
     */
    boolean isIdle();

    /**
     * Indicates whether the cursor is in the ACTIVE state.
     * @return true iff the cursor is ACTIVE.
     */
    boolean isActive();

    /**
     * Indicates whether the cursor is in the CLOSED state.
     * @return true iff the cursor is CLOSED.
     */
    boolean isClosed();
    
    /**
     * sets the cursor into an IDLE state when all active records 
     * are returned. 
     */
    void setIdle();
}
