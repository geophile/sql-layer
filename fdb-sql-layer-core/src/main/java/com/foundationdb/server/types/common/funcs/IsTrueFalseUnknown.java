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

public abstract class IsTrueFalseUnknown extends TScalarBase
{
    public static TScalar[] create (TClass boolType)
    {
        return new TScalar[]
        {
            new IsTrueFalseUnknown(boolType, "isTrue")
            {
                @Override
                protected void evaluate(ValueSource source, ValueTarget target)
                {
                    target.putBool(source.getBoolean(false));
                }
            },
            new IsTrueFalseUnknown(boolType, "isFalse")
            {
                @Override
                protected void evaluate(ValueSource source, ValueTarget target)
                {
                    target.putBool(!source.getBoolean(true));
                }
            },
            new IsTrueFalseUnknown(boolType, "isUnknown")
            {
                @Override
                protected void evaluate(ValueSource source, ValueTarget target)
                {
                    target.putBool(source.isNull());
                }
            }
        };
    }
   
    protected abstract void evaluate(ValueSource source, ValueTarget target);
    
    private final TClass boolType;
    private final String name;
    
    private IsTrueFalseUnknown(TClass boolType, String name)
    {
        this.boolType = boolType;
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(boolType, 0);
    }

     
    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        evaluate(inputs.get(0), output);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        // DOES NOTHING
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(boolType);
    }
}
