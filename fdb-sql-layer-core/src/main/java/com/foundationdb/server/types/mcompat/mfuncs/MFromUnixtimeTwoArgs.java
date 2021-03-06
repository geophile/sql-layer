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

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.DateTimeField;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import java.util.List;

import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public class MFromUnixtimeTwoArgs extends TScalarBase
{
    public static final TScalar INSTANCE = new MFromUnixtimeTwoArgs();
    
    private static final int RET_INDEX = 0;
    private static final int ERROR_INDEX = 1;
    
    private MFromUnixtimeTwoArgs() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.BIGINT, 0).covers(MString.VARCHAR, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        String ret = null;
        InvalidOperationException error = null;
            
        if ((error = (InvalidOperationException) context.preptimeObjectAt(ERROR_INDEX)) == null
                && (ret = (String)context.preptimeObjectAt(RET_INDEX)) == null)
        {
            Object objs[] = computeResult(inputs.get(0).getInt64(),
                                          inputs.get(1).getString(),
                                          context.getCurrentTimezone());
            
            ret = (String) objs[RET_INDEX];
            error = (InvalidOperationException) objs[ERROR_INDEX];
        }

        if (ret == null)
        {
            output.putNull();
            context.warnClient(error);
        }
        else
            output.putString(ret, null);
    }

    @Override
    public String displayName()
    {
        return "FROM_UNIXTIME";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TPreptimeValue formatArg = inputs.get(1);
                
                ValueSource format = formatArg.value();
                
                int length;
                
                // format is not literal
                // the length is format's precision * 10
                if (format == null)
                    length = formatArg.type().attribute(StringAttribute.MAX_LENGTH) * 10;
                else
                {
                    ValueSource unixTime = inputs.get(0).value();
                    
                    // if the unix time value is not literal, get the length
                    // from the format string
                    length = computeLength(format.getString());
                    
                    // if the unix time is available, get the actual length
                    if (unixTime != null)
                    {
                        Object prepObjects[] = computeResult(unixTime.getInt64(),
                                                            format.getString(),
                                                            context.getCurrentTimezone());
                        context.set(RET_INDEX, prepObjects[RET_INDEX]);
                        context.set(ERROR_INDEX, prepObjects[ERROR_INDEX]);
                        
                        // get the real length
                        if (prepObjects[RET_INDEX] != null)
                            length = ((String)prepObjects[RET_INDEX]).length();
                    }
                }
                return MString.VARCHAR.instance(length, anyContaminatingNulls(inputs));
            }
        });
    }
    
    private static Object[] computeResult(long unix, String format, String tz)
    {
        String st = null;
        InvalidOperationException error = null;
        
        try
        {
            st = DateTimeField.getFormatted(new MutableDateTime(unix * 1000L, DateTimeZone.forID(tz)),
                                            format);
        }
        catch (InvalidParameterValueException e)
        {
            st = null;
            error = e;
        }

        return new Object[]
        {
            st,
            error
        };
    }
    private static int computeLength(String st)
    {
        // TODO: parse the format string ONLY to compute the lenght
        // for now, assume it's the number of format specifier * 5
        return st.length() * 5 / 2;
    }
}
