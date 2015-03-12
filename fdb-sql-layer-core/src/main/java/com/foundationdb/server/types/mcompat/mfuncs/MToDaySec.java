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
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.ZeroFlag;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

@SuppressWarnings("unused")
public abstract class MToDaySec extends TScalarBase
{
    public static final TScalar TO_DAYS = new MToDaySec(MDateAndTime.DATETIME, "TO_DAYS")
    {
        @Override
        protected long evaluateInternal(long[] ymd) {
            long now = MDateAndTime.toJodaDateTime(ymd, DateTimeZone.UTC).getMillis();
            long days = (now - START) / MILLIS_PER_DAY;
            // Off by one for first year due to our start of 01-01 00-00
            return (days < DAYS_NEEDING_OFFSET) ? days + 1 : days;
        }
    };

    public static final TScalar TO_SECONDS = new MToDaySec(MDateAndTime.DATETIME, "TO_SECONDS")
    {
        @Override
        protected long evaluateInternal(long[] ymd) {
            long now = MDateAndTime.toJodaDateTime(ymd, DateTimeZone.UTC).getMillis();
            long seconds = (now - START) / MILLIS_PER_SEC;
            return (seconds < SECS_NEEDING_OFFSET) ? seconds + SEC_PER_DAY : seconds;
        }    
    };
    
    public static final TScalar TIME_TO_SEC = new MToDaySec(MDateAndTime.TIME, "TIME_TO_SEC")
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
        {
            long[] dt = MDateAndTime.decodeTime(inputs.get(0).getInt32());
            if(!MDateAndTime.isValidHrMinSec(dt, false, false)) {
                context.warnClient(new InvalidDateFormatException("time", MDateAndTime.timeToString(dt)));
                output.putNull();
            } else {
                int sign = MDateAndTime.isHrMinSecNegative(dt) ? -1 : 1;
                long value = Math.abs(dt[MDateAndTime.HOUR_INDEX]) * SEC_PER_HOUR +
                    Math.abs(dt[MDateAndTime.MIN_INDEX] * SEC_PER_MIN) +
                    Math.abs(dt[MDateAndTime.SEC_INDEX]);
                output.putInt64(sign * value);
            }
        }

        @Override
        protected long evaluateInternal(long[] ymd)
        {
            throw new AssertionError("Should not be used");
        }   
    };

    private static final int SEC_PER_MIN = 60;
    private static final int SEC_PER_HOUR = SEC_PER_MIN * 60;
    private static final int SEC_PER_DAY = SEC_PER_HOUR * 24;
    private static final int MILLIS_PER_SEC = 1000;
    private static final int MILLIS_PER_DAY = SEC_PER_DAY * MILLIS_PER_SEC;
    private static final long START = new DateTime(0, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();

    // Need offsets for first year due to Joda START of 0000-01-01 and not 0000-00-00
    private static final int DAYS_NEEDING_OFFSET = 365;
    private static final int SECS_NEEDING_OFFSET = SEC_PER_DAY * DAYS_NEEDING_OFFSET;

    private final TClass inputType;
    private final String name;

    abstract protected long evaluateInternal(long[] ymd);

    private MToDaySec(TClass inputType, String name)
    {
        this.inputType = inputType;
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(inputType, 0);
    }
    
    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        long[] dt = MDateAndTime.decodeDateTime(inputs.get(0).getInt64());
        if(MDateAndTime.isValidDateTime(dt, ZeroFlag.YEAR)) {
            output.putInt64(evaluateInternal(dt));
        } else {
            context.warnClient(new InvalidDateFormatException("datetime", MDateAndTime.dateTimeToString(dt)));
            output.putNull();
        }
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.BIGINT);
    }
}
