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
package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import java.util.List;

public class MConcatWS extends TScalarBase
{
    public static final TScalar INSTANCE = new MConcatWS();
    
    private MConcatWS() {}
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        // the function should have at least 2 args
        builder.vararg(MString.VARCHAR, 0, 1);
    }
    
    @Override
    protected boolean nullContaminates(int inputIndex)
    {
        return inputIndex == 0;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        String delim = inputs.get(0).getString();
        StringBuilder ret = new StringBuilder();

        for (int n = 1; n < inputs.size(); ++n)
        {
            ValueSource source = inputs.get(n);
            if (!source.isNull())
                ret.append(source.getString()).append(delim);
        }
        if (ret.length()!= 0)
            ret.delete(ret.length() - delim.length(),
                       ret.length());

        output.putString(ret.toString(), null);
    }

    @Override
    public String displayName()
    {
        return "CONCAT_WS";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                int dLen = inputs.get(0).type().attribute(StringAttribute.MAX_LENGTH);
                int len = 0;
                
                for (int n = 1; n < inputs.size(); ++n)
                    len += inputs.get(n).type().attribute(StringAttribute.MAX_LENGTH) + dLen;
                
                // delele the laste delimeter
                len -= dLen;
                
                return MString.VARCHAR.instance(len, anyContaminatingNulls(inputs));
            }
        });
    }
}
