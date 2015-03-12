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
package com.foundationdb.qp.row;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.ValuesRowType;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public final class ImmutableRowTest {
    @Test
    public void basic() {
        Value vh1 = new Value(MNumeric.BIGINT.instance(false), 1L);
        Value vh2 = new Value(MString.varchar(), "right");
        Row row = new ImmutableRow (rowType(MNumeric.BIGINT.instance(false), MString.varchar()), Arrays.asList(vh1, vh2).iterator());
        vh1.putInt64(50L);
        vh2.putString("wrong", null);
        assertEquals("1", 1L, row.value(0).getInt64());
        assertEquals("2", "right", row.value(1).getString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooFewInputs() {
        new ImmutableRow(rowType(MNumeric.INT.instance(false)), Collections.<ValueSource>emptyList().iterator());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooManyInputs() {
        new ImmutableRow(
                rowType(MNumeric.BIGINT.instance(false)),
                Arrays.asList(
                        new Value(MNumeric.BIGINT.instance(false), 1L),
                        new Value(MString.varchar(), "bonus")).iterator()
        );
    }

    @Test(expected = IllegalStateException.class)
    public void wrongInputType() {
        new ImmutableRow(
                rowType(MNumeric.INT.instance(false)),
                Collections.singleton(new Value(MNumeric.INT.instance(false), "1L")).iterator()
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void tryToClear() {
        ImmutableRow row = new ImmutableRow(
                rowType(MNumeric.INT.instance(false)),
                Collections.singleton(new Value(MString.varchar(), "1L")).iterator()
        );
        row.clear();
    }

    
    @Test(expected = IllegalStateException.class)
    public void tryToGetHolder() {
        ImmutableRow row = new ImmutableRow(
                rowType(MString.varchar()),
                Collections.singleton(new Value(MString.varchar(), "1L")).iterator()
        );
        row.valueAt(0);
    }

    private RowType rowType(TInstance... types) {
        return new ValuesRowType (null, 1, types);
    }
}
