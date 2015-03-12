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
package com.foundationdb.qp.rowtype;

import com.foundationdb.server.types.TInstance;

import java.util.Arrays;

import static org.junit.Assert.*;

public final class RowTypeChecks {

    public static void checkRowTypeFields(String message, RowType rowType, TInstance... expected) {
        assertEquals (rowType.nFields(), expected.length);
        TInstance[] actual = new TInstance[rowType.nFields()];
        for (int i = 0; i < actual.length; ++i) {
            actual[i] = rowType.typeAt(i);
        }
        
        if (!Arrays.equals(expected, actual)) {
            assertEquals (message, Arrays.toString(expected), Arrays.toString(actual));
            assertArrayEquals(message, expected, actual);
        }
    }
}
