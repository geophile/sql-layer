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

import com.foundationdb.server.error.InvalidCharToNumException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class InetAton extends TScalarBase
{
    private static final long FACTORS[] = {16777216L,  65536, 256};        
    
    private final TClass argType;
    private final TClass returnType;
    
    public InetAton(TClass tclass, TClass returnType)
    {
        assert tclass instanceof TString : "expecting a string class";
        this.argType = tclass;
        
        // TODO: assert returnType instaceof BIGINT ...
        this.returnType = returnType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(argType, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        String tks[] = (inputs.get(0).getString()).split("\\.");
        if (tks.length > 4)
            output.putNull();
        else
            try
            {
                int last = tks.length - 1;
                short val = Short.parseShort(tks[last]);
                long ret = val;
                
                if (ret < 0 || ret > 255) output.putNull();
                else if (tks.length == 1) output.putInt64(ret);
                else
                {
                    for (int i = 0; i < last; ++i)
                        if ((val = Short.parseShort(tks[i])) < 0 || val > 255)
                        {
                            output.putNull();
                            return;
                        }
                        else
                            ret += val * FACTORS[i];
                    output.putInt64(ret);
                }
            }
            catch (NumberFormatException e)
            {
                context.warnClient(new InvalidCharToNumException(e.getMessage()));
                output.putNull();
            }
    }

    @Override
    public String displayName()
    {
        return "INET_ATON";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(returnType);
    }
}
