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
package com.foundationdb.sql.embedded;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class JDBCException extends SQLException
{
    public JDBCException() {
        super();
    }

    public JDBCException(String reason, ErrorCode code) {
        super(reason, code.getFormattedValue());
    }
    
    public JDBCException(InvalidOperationException cause) {
        super(cause.getShortMessage(), cause.getCode().getFormattedValue(), cause);
    }
    
    public JDBCException(String reason, InvalidOperationException cause) {
        super(reason, cause.getCode().getFormattedValue(), cause);
    }
    
    public JDBCException(Throwable cause) {
        super(cause);
    }

    public JDBCException(String reason, Throwable cause) {
        super(reason, cause);
    }

    // Allow outer layer to throw SQLException through inner layer that does not.
    protected static class Wrapper extends RuntimeException {
        public Wrapper(SQLException cause) {
            super(cause);
        }
    }

    protected static RuntimeException wrapped(String reason, ErrorCode code) {
        return new Wrapper(new JDBCException(reason, code));
    }

    protected static RuntimeException throwUnwrapped(RuntimeException ex) throws SQLException {
        if (ex instanceof Wrapper)
            throw (SQLException)ex.getCause();
        if (ex instanceof InvalidOperationException)
            throw new JDBCException((InvalidOperationException)ex);
        if (ex instanceof UnsupportedOperationException)
            throw new SQLFeatureNotSupportedException(ex.getMessage());
        return ex;
    }
}
