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
package com.foundationdb.server.types.mcompat.aggr;

import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TFixedTypeAggregator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

public abstract class MBit extends TFixedTypeAggregator {
    
    public static final TAggregator[] INSTANCES = {
        // BIT_AND
        new MBit("BIT_AND") {

            @Override
            long process(long i0, long i1) {
                return i0 & i1;
            }
        }, 
        // BIT_OR
        new MBit("BIT_OR") {

            @Override
            long process(long i0, long i1) {
                return i0 | i1;
            }
        }, 
        // BIT_XOR
        new MBit("BIT_XOR") {

            @Override
            long process(long i0, long i1) {
                return i0 ^ i1;
            }
        }
    };
    
    abstract long process(long i0, long i1);

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object o) {
        if (!source.isNull()) {
            long incoming = source.getInt64();
            if (!state.hasAnyValue()) {
                state.putInt64(incoming);
            }
            else {
                long previousState = state.getInt64();
                state.putInt64(process(previousState, incoming));
            }
        }    
    }

    @Override
    public void emptyValue(ValueTarget state) {
        state.putNull();
    }

    private MBit(String name) {
        super(name, MNumeric.BIGINT);
    }
}
