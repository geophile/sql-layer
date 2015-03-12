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

public final class CursorLifecycle
{
    public static void checkIdle(CursorBase cursor)
    {
        if (!cursor.isIdle()) {
            throw new WrongStateException(CursorState.IDLE.toString(), cursor);
        }
    }

    public static void checkIdleOrActive(CursorBase cursor)
    {
        if (cursor.isClosed()) {
            String state = CursorState.IDLE.toString() + " OR " + CursorState.ACTIVE.toString();
            throw new WrongStateException(state, cursor);
        }
    }

    public static void checkClosed (CursorBase cursor)
    {
        if (!cursor.isClosed()) {
            throw new WrongStateException (CursorState.CLOSED.toString(), cursor);
        }
    }
    
    private static String cursorState(CursorBase cursor)
    {
        return cursor.isIdle() ? CursorState.IDLE.toString() : 
            cursor.isActive() ? CursorState.ACTIVE.toString() : CursorState.CLOSED.toString();
    }

    public enum CursorState {
        CLOSED,
        IDLE,
        ACTIVE
    }

    public static class WrongStateException extends RuntimeException
    {
        WrongStateException(String expectedState, CursorBase cursor)
        {
            super(String.format("Cursor should be %s but is actually %s", expectedState, cursorState(cursor)));
        }
    }
}
