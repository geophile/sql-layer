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
package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class BinaryBit extends TScalarBase {

    static enum BitOperator 
    {
        BITAND
        {
            @Override
            long evaluate(long a0, long a1)
            {
                return a0 & a1;
            }
        },
        BITOR
        {
            @Override
            long evaluate(long a0, long a1)
            {
                return a0 | a1;
            }
        },
        BITXOR
        {
            @Override
            long evaluate(long a0, long a1)
            {
                return a0 ^ a1;
            }
        },
        LEFTSHIFT
        {
            @Override
            long evaluate(long a0, long a1)
            {
                return (a1 < 0) ? 0 : (a0 << a1);
            }
        },
        RIGHTSHIFT
        {
            @Override
            long evaluate(long a0, long a1)
            {
                return (a1 < 0) ? 0 : (a0 >> a1);
            }
        },
        BITNOT
        {
            @Override
            long evaluate(long a0, long a1)
            {
                // doEvaluate method overrides this
                throw new AssertionError();
            }
        },
        BIT_COUNT
        {
            @Override
            long evaluate(long a0, long a1)
            {
                // doEvaluate method overrides this
                throw new AssertionError();
            }
        };

        abstract long evaluate(long a0, long a1);
    }
    
    public static final TScalar[] create(TClass longType) {
        return new TScalar[] {
            new BinaryBit(BitOperator.BITAND, longType),
            new BinaryBit(BitOperator.BITOR, longType),
            new BinaryBit(BitOperator.BITXOR, longType),
            new BinaryBit(BitOperator.LEFTSHIFT, longType),
            new BinaryBit(BitOperator.RIGHTSHIFT, longType),
            new BinaryBit(BitOperator.BITNOT, longType) {           
                @Override
                protected void buildInputSets(TInputSetBuilder builder) {
                    builder.covers(longType, 0);
                }

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
                    output.putInt64(~inputs.get(0).getInt64());
                }
            },
            new BinaryBit(BitOperator.BIT_COUNT, longType) {
                @Override
                protected void buildInputSets(TInputSetBuilder builder) {
                    builder.covers(longType, 0);
                }

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
                    long input = inputs.get(0).getInt64();
                    output.putInt64(Long.bitCount(input));
                }
            }
        };
    }
    
    private final BitOperator bitOp;
    protected final TClass longType;
    private BinaryBit(BitOperator bitOp, TClass longType) {
        this.bitOp = bitOp;
        this.longType = longType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(longType, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        long a0 = inputs.get(0).getInt64();
        long a1 = inputs.get(1).getInt64();
        long ret = bitOp.evaluate(a0, a1);
        output.putInt64(ret);
    }

    @Override
    public String displayName() {
        return bitOp.name();
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(longType);
    }
}
