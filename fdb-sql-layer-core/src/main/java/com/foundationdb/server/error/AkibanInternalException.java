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

/**
 * Akiban Internal Exceptions are errors internal to the Akiban Server, 
 * which should never happen but might. These are Exceptions on the 
 * same order as @see NullPointerException, indicating bugs in the code. 
 * These are defined to give maximum visibility to these problems as 
 * quickly and with as much information as possible.  
 * @author tjoneslo
 *
 */
public class AkibanInternalException extends RuntimeException {
    
    public AkibanInternalException(String message) {
        super (message);
    }
    
    public AkibanInternalException(String message, Throwable cause)  {
        super (message, cause);
    }

    public ErrorCode getCode() { 
        return ErrorCode.INTERNAL_ERROR;
    }
}
