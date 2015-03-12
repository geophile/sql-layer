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

import com.foundationdb.server.error.InvalidDateFormatException;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.ZeroFlag;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

@SuppressWarnings("unused")
public class MConvertTZ extends TScalarBase
{
    public static final TScalar INSTANCE = new MConvertTZ();

    private MConvertTZ()
    {}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MDateAndTime.DATETIME, 0).covers(MString.VARCHAR, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        long original = inputs.get(0).getInt64();
        long[] ymd = MDateAndTime.decodeDateTime(original);
        if(!MDateAndTime.isValidDateTime(ymd, ZeroFlag.YEAR)) {
            output.putNull();
        } else {
            try {
                DateTimeZone fromTz = MDateAndTime.parseTimeZone(inputs.get(1).getString());
                DateTimeZone toTz = MDateAndTime.parseTimeZone(inputs.get(2).getString());
                MutableDateTime date = MDateAndTime.toJodaDateTime(ymd, fromTz);
                // If the value falls out of the supported range of the TIMESTAMP
                // when converted from fromTz to UTC, no conversion occurs.
                date.setZone(DateTimeZone.UTC);
                final long converted;
                if(MDateAndTime.isValidTimestamp(date)) {
                    date.setZone(toTz);
                    converted = MDateAndTime.encodeDateTime(date);
                } else {
                    converted = original;
                }
                output.putInt64(converted);
            } catch(InvalidDateFormatException e) {
                context.warnClient(e);
                output.putNull();
            }
        }
    }

    @Override
    public String displayName()
    {
        return "CONVERT_TZ";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MDateAndTime.DATETIME);
    }
}
