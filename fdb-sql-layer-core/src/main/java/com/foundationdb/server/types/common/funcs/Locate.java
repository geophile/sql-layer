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

public abstract class Locate extends TScalarBase
{
    public static TScalar create2ArgOverload(final TClass stringType, final TClass intType, String name)
    {
        return new Locate(intType, name)
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(stringType, 0).covers(stringType, 1);
            }
        };
    }
    
    public static TScalar create3ArgOverload(final TClass stringType, final TClass intType, String name)
    {
        return new Locate(intType, name)
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(stringType, 0).covers(stringType, 1).covers(intType, 2);
            }
        };
    }
    
    private final TClass intType;
    private final String name;
    
    Locate(TClass intType, String name)
    {
        this.intType = intType;
        this.name = name;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        String str = inputs.get(1).getString();
        String substr = inputs.get(0).getString();

        int index = 0;
        if (inputs.size() == 3)
        {
            index = inputs.get(2).getInt32() - 1; // mysql uses 1-based indexing
            // invalid index => return 0 as the result
            if (index < 0 || index > str.length())
            {
                output.putInt32(0);
                return;
            }
        }
        output.putInt32(1 + str.indexOf(substr, index));
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(intType);
    }
}

