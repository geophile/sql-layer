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
package com.foundationdb.server.error;

import com.foundationdb.FDBException;

public class FDBAdapterException extends StoreAdapterRuntimeException
{
    /** For use with errors originating from the FDB client. */
    public FDBAdapterException(FDBException ex) {
        this(ErrorCode.FDB_ERROR, ex);
    }

    /** For use with FDB-specific SQL Layer errors that do not originate from the FDB client. */
    public FDBAdapterException(String msg) {
        this(ErrorCode.FDB_ERROR, msg);
    }

    protected FDBAdapterException(ErrorCode errorCode, FDBException ex) {
        this(errorCode, String.format("%d - %s", ex.getCode(), ex.getMessage()));
        initCause(ex);
    }

    protected FDBAdapterException(ErrorCode errorCode, String desc) {
        super(errorCode, desc);
    }
    
    protected FDBAdapterException (ErrorCode code, Long i, Long j, Long k, Long l) {
        super (code, i, j, k, l);
    }
}
