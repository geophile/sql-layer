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
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.List;

public abstract class LeftRight extends TScalarBase
{
    public static TScalar getLeft(TClass stringType, TClass intType)
    {
        return new LeftRight(stringType, intType, "LEFT", "getLeft")
        {

            @Override
            String getSubstring(String st, int length)
            {
                return st.substring(0, length);
            }
            
        };
    }

    public static TScalar getRight(TClass stringType, TClass intType)
    {
        return new LeftRight(stringType, intType, "RIGHT", "getRight")
        {
            @Override
            String getSubstring(String st, int length)
            {
                return st.substring(st.length() - length, st.length());
            }
        };
    }
    
    abstract String getSubstring(String st, int length);
    
    private final TClass stringType;
    private final TClass intType;
    private final String name;
    private final String registeredName;
    
    private LeftRight (TClass stringType, TClass intType, String name, String regname)
    {
        this.stringType = stringType;
        this.intType = intType;
        this.name = name;
        this.registeredName = regname;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(stringType, 0).covers(intType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        String st = inputs.get(0).getString();
        int len = inputs.get(1).getInt32();

        // adjust the length
        len = len < 0 
                ? 0
                : len > st.length() ? st.length() : len;

        output.putString(getSubstring(st, len), null);
    }
    
    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public String[] registeredNames()
    {
        return new String[] {registeredName};
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TPreptimeValue len = inputs.get(1);

                // if second argument is not available or is null
                if (len.value() == null || len.value().isNull())
                {
                    TPreptimeValue st = inputs.get(0);
                    
                    // if the string is also not available
                    // the return the precision of the string's type
                    if (st.value() == null || st.value().isNull())
                        return st.type().withNullable(false);
                    else // if the string is available, return its length
                        return stringType.instance((st.value().getString()).length(), anyContaminatingNulls(inputs));
                }
                else
                    return stringType.instance(len.value().getInt32(), anyContaminatingNulls(inputs));
            }
            
        });
    }
    
}
