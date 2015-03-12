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
package com.foundationdb.sql.server;

import com.foundationdb.server.types.TInstance;

/** A type according to the server's regime.
 */
public abstract class ServerType
{
    public enum BinaryEncoding {
        NONE, INT_8, INT_16, INT_32, INT_64, FLOAT_32, FLOAT_64, STRING_BYTES,
        BINARY_OCTAL_TEXT, BOOLEAN_C, 
        TIMESTAMP_FLOAT64_SECS_2000_NOTZ, TIMESTAMP_INT64_MICROS_2000_NOTZ,
        DAYS_2000, TIME_FLOAT64_SECS_NOTZ, TIME_INT64_MICROS_NOTZ,
        DECIMAL_PG_NUMERIC_VAR, UUID
    }

    private final TInstance type;

    protected ServerType(TInstance type) {
        this.type = type;
    }
    
    public TInstance getType() {
        return type;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.NONE;
    }

    @Override
    public String toString() {
        if (type == null)
            return "null";
        else
            return type.toStringConcise(false);
    }

}
