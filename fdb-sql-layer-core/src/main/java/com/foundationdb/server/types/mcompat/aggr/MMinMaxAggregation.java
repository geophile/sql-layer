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
import com.foundationdb.server.types.TAggregatorBase;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;

public class MMinMaxAggregation extends TAggregatorBase {

    private final MType mType;
    
    private enum MType {
        MIN() {
            @Override
            boolean condition(int a) {
                return a < 0;
            }   
        }, 
        MAX() {
            @Override
            boolean condition(int a) {
                return a > 0;
            }
        };
        abstract boolean condition (int a);
    }

    public static final TAggregator MIN = new MMinMaxAggregation(MType.MIN);
    public static final TAggregator MAX = new MMinMaxAggregation(MType.MAX);
    
    private MMinMaxAggregation(MType mType) {
        super(mType.name(), null);
        this.mType = mType;
    }

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object o) {
        if (source.isNull())
            return;
        if (!state.hasAnyValue()) {
            ValueTargets.copyFrom(source, state);
            return;
        }
        TClass tClass = type.typeClass();
        assert stateType.typeClass().equals(tClass) : "incompatible types " + type + " and " + stateType;
        int comparison = TClass.compare(type, source, stateType, state);
        if (mType.condition(comparison))
            ValueTargets.copyFrom(source, state);
    }

    @Override
    public void emptyValue(ValueTarget state) {
        state.putNull();
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }
}
