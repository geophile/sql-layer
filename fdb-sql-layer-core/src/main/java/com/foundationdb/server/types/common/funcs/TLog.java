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
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.TScalar;

public class TLog extends TScalarBase
{
    static final double ln2 = Math.log(2);
    
    public static TScalar[] create(TClass argType)
    {
        LogType values[] = LogType.values();
        TScalar ret[] = new TScalar[values.length];
        
        for (int n = 0; n < ret.length; ++n)
            ret[n] = new TLog(values[n], argType);
        return ret;
    }

    static enum LogType
    {
        LN
        {
            @Override
            double evaluate(double input)
            {
                return Math.log(input);
            }
        },
        LOG
        {
            @Override
            double evaluate(double input)
            {
                return Math.log(input);
            }
        },
        LOG10
        {
            @Override
            double evaluate(double input)
            {
                return Math.log10(input);
            }
        },  
        LOG2
        {
            @Override
            double evaluate(double input)
            {
                return Math.log(input)/ln2;
            }
        };
        
        abstract double evaluate(double input);
        
        boolean isValid(double input)
        {
            return input > 0;
        }
    }

    private final LogType logType;
    private final TClass argType;
    
    TLog (LogType logType, TClass argType)
    {
        this.logType = logType;
        this.argType = argType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(argType, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        double input = inputs.get(0).getDouble();
        if (logType.isValid(input))
            output.putDouble(logType.evaluate(input));
        else
            output.putNull();
    }

    @Override
    public String displayName()
    {
        return logType.name();
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(argType);
    }
}
