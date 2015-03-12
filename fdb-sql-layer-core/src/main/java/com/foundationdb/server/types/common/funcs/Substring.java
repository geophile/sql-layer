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
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.List;

public abstract class Substring extends TScalarBase
{
    public static TScalar[] create(TClass strType, TClass intType)
    {
        return new TScalar[]
        {
            new Substring(strType, intType, new int[] {1}) // 2 args: SUBSTR(<STRING>, <OFFSET>)
            {
                @Override
                protected Integer getLength(LazyList<? extends ValueSource> inputs)
                {
                    return null;
                }   
            },
            new Substring(strType, intType, new int[] {1, 2}) // 3 args: SUBSTR(<STRING>, <OFFSET>, <LENGTH>)
            {
                @Override
                protected Integer getLength(LazyList<? extends ValueSource> inputs)
                {
                    return inputs.get(2).getInt32();
                }   
            },
            
        };
    }
    
    protected abstract Integer getLength (LazyList<? extends ValueSource> inputs);
    
    private final TClass strType;
    private final TClass intType;
    private final int covering[];
    
    private Substring(TClass strType, TClass intType, int covering[])
    {
        this.strType = strType;
        this.intType = intType;
        this.covering = covering;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(strType, 0).covers(intType, covering);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        output.putString(getSubstr(inputs.get(0).getString(),
                                   inputs.get(1).getInt32(),
                                   getLength(inputs)),
                                   null);
    }

    @Override
    public String displayName()
    {
        return "SUBSTRING";
    }

    @Override
    public String[] registeredNames()
    {
        return new String[] {"SUBSTRING", "SUBSTR", "MID"};
    }
    
    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                int strLength = inputs.get(0).type().attribute(StringAttribute.MAX_LENGTH);
                // usage: SUBSTR (<STRING> , <OFFSET> [, <LENGTH>] )
                int length = strLength;
                ValueSource lenArg;
                // check if <LENGTH> is available
                if (inputs.size() == 3 && (lenArg = inputs.get(2).value()) != null
                                       && !lenArg.isNull()) {
                    length = lenArg.getInt32();
                }
                return strType.instance(length > strLength ? strLength : length, anyContaminatingNulls(inputs));
            }
        });
    }
    
    private static String getSubstr(String st, int from, Integer length) {
        // if str is empty or <from> and <length> is outside of reasonable index
        // 
        // Note negative index is acceptable for <from>, but its absolute value has
        // to be within [1, str.length] (mysql index starts at 1)
        if (st.isEmpty() || from == 0 || (length != null && length <= 0)) {
            return "";
        }
        try {
            if (from < 0) {
                from = st.offsetByCodePoints(st.length(), from);
            } else {
                from = st.offsetByCodePoints(0, from - 1);
            }
        }
        catch (IndexOutOfBoundsException ex) {
            return "";
        }
        if (length == null) {
            return st.substring(from);
        }
        int to;
        try {
            to = st.offsetByCodePoints(from, length);
        } catch (IndexOutOfBoundsException ex) {
            to = st.length();
        }
        return st.substring(from, to);
    }
}
