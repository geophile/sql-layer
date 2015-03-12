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
package com.foundationdb.util;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;

import java.sql.SQLException;
import java.util.List;

public final class Exceptions {
    /**
     * Throws the given throwable, downcast, if it's of the appropriate type
     *
     * @param t the exception to check
     * @param cls  the class to check for and cast to
     * @throws T the e instance, cast down
     */
    public static <T extends Throwable> void throwIfInstanceOf(Throwable t, Class<T> cls) throws T {
        if (cls.isInstance(t)) {
            throw cls.cast(t);
        }
    }

    /**
     * <p>Always throws something. If {@code t} is a RuntimeException, it simply gets thrown. If it's a checked
     * exception, it gets wrapped in a RuntimeException. If it's an Error, it simply gets thrown. And if somehow
     * it's something else, that thing is wrapped in an Error and thrown.</p>
     *
     * <p>The return value of Error is simply as a convenience for methods that return a non-void type; you can
     * invoke {@code throw throwAlways(t);} to indicate to the compiler that the method's control will end there.</p>
     * @param t the throwable to throw, possibly wrapped if needed
     * @return nothing, since something is always thrown from this method.
     */
    public static Error throwAlways(Throwable t) {
        throwIfInstanceOf(t,RuntimeException.class);
        if (t instanceof Exception) {
            throw new RuntimeException(t);
        }
        throwIfInstanceOf(t, Error.class);
        throw new Error("not a RuntimeException, checked exception or Error?!", t);
    }

    public static Error throwAlways(List<? extends Throwable> throwables, int index) {
        Throwable t = throwables.get(index);
        throw throwAlways(t);
    }

    public static boolean isRollbackException(Throwable t) {
        if(t instanceof SQLException) {
            String sqlState = ((SQLException)t).getSQLState();
            try {
                ErrorCode code = ErrorCode.valueOfCode(sqlState);
                return code.isRollbackClass();
            } catch(IllegalArgumentException e) {
                // Not a valid SQLState
                return false;
            }
        }
        return (t instanceof InvalidOperationException) &&
               ((InvalidOperationException)t).getCode().isRollbackClass();
    }
}
