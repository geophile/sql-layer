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

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TFixedTypeAggregator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

public class MStdDevVarAggregate extends TFixedTypeAggregator
{
    enum Func {
        // These are the actual aggregate functions. They are only
        // here so that we can distinguish from regular functions
        // early enough for the optimizer to transform them.
        VAR_POP, VAR_SAMP, STDDEV_POP, STDDEV_SAMP,
        // These are the partial aggregators.
        SUM, SUM_SQUARE 
    }

    public static final TAggregator[] INSTANCES = {
        new MStdDevVarAggregate(Func.VAR_POP, "VAR_POP"),
        new MStdDevVarAggregate(Func.VAR_SAMP, "VAR_SAMP"),
        new MStdDevVarAggregate(Func.STDDEV_POP, "STDDEV_POP"),
        new MStdDevVarAggregate(Func.STDDEV_SAMP, "STDDEV_SAMP"),
        new MStdDevVarAggregate(Func.SUM, "_VAR_SUM"),
        new MStdDevVarAggregate(Func.SUM_SQUARE, "_VAR_SUM_2")
    };

    private final Func func;

    private MStdDevVarAggregate(Func func, String name) {
        super(name, MApproximateNumber.DOUBLE);
        this.func = func;
    }

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object del)
    {
        if (source.isNull())
            return;
        double x = source.getDouble();
        double sum = state.hasAnyValue() ? state.getDouble() : 0;
        switch (func) {
        case SUM:
            sum += x;
            break;
        case SUM_SQUARE:
            sum += x * x;
            break;
        default:
            throw new AkibanInternalException("Aggregator for " + displayName() + " should have been optimized out");
        }
        state.putDouble(sum);
    }

    @Override
    public void emptyValue(ValueTarget state)
    {
        state.putNull();
    }
}
