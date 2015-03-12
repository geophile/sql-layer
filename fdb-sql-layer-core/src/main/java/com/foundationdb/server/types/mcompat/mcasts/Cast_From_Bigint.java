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
package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.error.InvalidDateFormatException;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;

import static com.foundationdb.server.types.mcompat.mcasts.MNumericCastBase.*;

@SuppressWarnings("unused")
public class Cast_From_Bigint
{
    public static final TCast TO_TINYINT = new FromInt64ToInt8(MNumeric.BIGINT, MNumeric.TINYINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_TINYINT = new FromInt64ToUnsignedInt8(MNumeric.BIGINT, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN);

    public static final TCast TO_SMALLINT = new FromInt64ToInt16(MNumeric.BIGINT, MNumeric.SMALLINT, false, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_SMALLINT = new FromInt64ToUnsignedInt16(MNumeric.BIGINT, MNumeric.SMALLINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_MEDIUM_INT = new FromInt64ToInt32(MNumeric.BIGINT, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt64ToUnsignedInt32(MNumeric.BIGINT, MNumeric.MEDIUMINT_UNSIGNED, false, Constantness.UNKNOWN);

    public static final TCast TO_INT = new FromInt64ToInt32(MNumeric.BIGINT, MNumeric.INT, false, Constantness.UNKNOWN);

    public static final TCast TO_INT_UNSIGNED = new FromInt64ToUnsignedInt32(MNumeric.BIGINT, MNumeric.INT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt64ToInt64(MNumeric.BIGINT, MNumeric.BIGINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt64ToDecimal(MNumeric.BIGINT, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt64ToDouble(MNumeric.BIGINT, MApproximateNumber.DOUBLE, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DATE = new TCastBase(MNumeric.BIGINT, MDateAndTime.DATE, Constantness.UNKNOWN)
    {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            long input = source.getInt64();
            try {
                long[] ymd = MDateAndTime.parseDate(input);
                int output = MDateAndTime.encodeDate(ymd);
                target.putInt32(output);
            } catch(InvalidDateFormatException e) {
                context.warnClient(e);
                target.putNull();
            }
        }
    };

    public static final TCast TO_DATETIME = new TCastBase(MNumeric.BIGINT, MDateAndTime.DATETIME, Constantness.UNKNOWN)
    {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            long input = source.getInt64();
            try {
                long[] ymdhms = MDateAndTime.parseDateTime(input);
                long output = MDateAndTime.encodeDateTime(ymdhms);
                target.putInt64(output);
            } catch(InvalidDateFormatException e) {
                context.warnClient(e);
                target.putNull();
            }
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MNumeric.BIGINT, MDateAndTime.TIMESTAMP, Constantness.UNKNOWN)
    {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            // TIMESTAMPE is underlied by INT32
            target.putInt32((int)MDateAndTime.encodeTimestamp(source.getInt64(), context));
        }
    };

    public static final TCast TO_TIME = new TCastBase(MNumeric.BIGINT, MDateAndTime.TIME, Constantness.UNKNOWN)
    {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            long raw = source.getInt64();
            long ymd[] = MDateAndTime.decodeTime(raw);
            if(!MDateAndTime.isValidHrMinSec(ymd, false, false)) {
                context.warnClient(new InvalidDateFormatException("TIME", Long.toString(raw)));
                target.putNull();
            } else {
                target.putInt32((int)CastUtils.getInRange(MDateAndTime.TIME_MAX, MDateAndTime.TIME_MIN, raw, context));
            }
        }
    };
}
