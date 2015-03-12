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

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.Random;

public abstract class Rand extends TScalarBase {

    private static final int RAND_INDEX = 0; // the cached Random Object's index

    public static TScalar[] create(TClass inputType, TClass resultType)
    {
        return new TScalar[]
        {
            new Rand(inputType, resultType)
            {
                @Override
                protected Random getRandom(LazyList<? extends ValueSource> inputs)
                {
                    return new Random();
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    // does nothing. Takes 0 arg
                }
            },
            new Rand(inputType, resultType)
            {
                @Override
                protected Random getRandom(LazyList<? extends ValueSource> inputs)
                {
                    ValueSource input = inputs.get(0);
                    if (input.isNull())
                        return new Random();
                    else
                        return new Random(inputs.get(0).getInt64());
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    builder.covers(inputType, 0);
                }
            }
        };
    }
    
    protected abstract  Random getRandom (LazyList<? extends ValueSource> inputs);
    protected final TClass inputType;
    protected final TClass resultType;

    protected Rand(TClass inputType, TClass resultType) {
        if (inputType.underlyingType() != UnderlyingType.INT_64
                || resultType.underlyingType() != UnderlyingType.DOUBLE)
            throw new IllegalArgumentException("Wrong types");
        this.inputType = inputType;
        this.resultType = resultType;
    }

    @Override
    protected boolean nullContaminates(int inputIndex)
    {
        return false;
    }
    
    @Override
    protected boolean neverConstant()
    {
        return true;
    }
    
    @Override
    public String displayName() {
        return "RAND";
    }

    @Override
    public String[] registeredNames() {
        return new String[] { "rand", "random" };
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(resultType);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget out)
    {
        Random rand;
        if (context.hasExectimeObject(RAND_INDEX))
            rand = (Random) context.exectimeObjectAt(RAND_INDEX);
        else
            context.putExectimeObject(RAND_INDEX, rand = getRandom(inputs));

        out.putDouble(rand.nextDouble());
    }
}
