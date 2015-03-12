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

import com.foundationdb.server.error.OverflowException;
import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TFixedTypeAggregator;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

import java.util.List;

public class MSum extends TFixedTypeAggregator {

    private final SumType sumType;
    
    private enum SumType {
        BIGINT(MNumeric.BIGINT) {
            @Override
            void input(TInstance type, ValueSource source, TInstance stateType, Value state) {
                long oldState = source.getInt64();
                long input = state.getInt64();
                long sum = oldState + input;
                if (oldState > 0 && input > 0 && sum <= 0) {
                    throw new OverflowException();
                } else if (oldState < 0 && input < 0 && sum >= 0) {
                    throw new OverflowException();
                } else {
                    state.putInt64(sum);
                }
            }
        }, 
        DOUBLE(MApproximateNumber.DOUBLE) {
            @Override
            void input(TInstance type, ValueSource source, TInstance stateType, Value state) {
                double oldState = source.getDouble();
                double input = state.getDouble();
                double sum = oldState + input;
                if (Double.isInfinite(sum) && !Double.isInfinite(oldState) && !Double.isInfinite(input)) {
                    throw new OverflowException();
                } else {
                    state.putDouble(sum);
                }
            }
        },
        DECIMAL(MNumeric.DECIMAL) {
            @Override
            void input(TInstance type, ValueSource source, TInstance stateType, Value state) {
                BigDecimalWrapper oldState = TBigDecimal.getWrapper(source, type);
                BigDecimalWrapper input = TBigDecimal.getWrapper(state, type);
                state.putObject(oldState.add(input));
            }
        }
        ;
        abstract void input(TInstance type, ValueSource source, TInstance stateType, Value state);
        private final TClass typeClass;
        
        private SumType(TClass typeClass) {
            this.typeClass = typeClass;
        }
    }
    
    public static final TAggregator[] INSTANCES = {
        new MSum(SumType.DECIMAL),
        new MSum(SumType.DOUBLE),
        new MSum(SumType.BIGINT)
    };
    
    private MSum(SumType sumType) {
        super("sum", sumType.typeClass);
        this.sumType = sumType;
    }

    // Want integers to all sum as long and floats as double, but decimals as the
    // particular precision of the input.

    @Override
    public List<TInputSet> inputSets() {
        if (sumType != SumType.DECIMAL)
            return super.inputSets();

        TInputSetBuilder builder = new TInputSetBuilder();
        builder.pickingCovers(inputClass(), 0);
        return builder.toList();
    }
    
    @Override
    public TOverloadResult resultType() {
        if (sumType != SumType.DECIMAL)
            return super.resultType();

        return TOverloadResult.picking();
    }

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object o) {
            if (source.isNull())
                return;
        if (!state.hasAnyValue())
            ValueTargets.copyFrom(source, state);
        else
            sumType.input(type, source, stateType, state);
    }

    @Override
    public void emptyValue(ValueTarget state) {
        state.putNull();
    }
}
