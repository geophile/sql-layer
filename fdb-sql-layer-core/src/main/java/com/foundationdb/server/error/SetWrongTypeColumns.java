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

import java.util.Arrays;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;

public class SetWrongTypeColumns extends InvalidOperationException {
    public SetWrongTypeColumns (RowType rt1, RowType rt2) {
        super(ErrorCode.SET_WRONG_TYPE_COLUMNS,    
                tInstanceOf(rt1),
                tInstanceOf(rt2));
    }

    private static String tInstanceOf (RowType rt) {
        TInstance[] result = new TInstance[rt.nFields()];
        for (int i = 0; i < result.length; ++i) {
            result[i] = rt.typeAt(i);
        }
        return Arrays.toString(result);
    }

    
}
