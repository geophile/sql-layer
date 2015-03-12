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

import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;

import static com.foundationdb.server.types.mcompat.mcasts.MNumericCastBase.*;

public class Cast_From_Timestamp
{
    public static final TCast TO_TINYINT = new FromInt32ToInt8(MDateAndTime.TIMESTAMP, MNumeric.TINYINT, false, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_TINYINT = new FromInt32ToUnsignedInt8(MDateAndTime.TIMESTAMP, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_SMALLINT = new FromInt32ToInt16(MDateAndTime.TIMESTAMP, MNumeric.SMALLINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_SMALINT = new FromInt32ToUnsignedInt16(MDateAndTime.TIMESTAMP, MNumeric.SMALLINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_MEDIUMINT = new FromInt32ToInt32(MDateAndTime.TIMESTAMP, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt32ToUnsignedInt32(MDateAndTime.TIMESTAMP, MNumeric.MEDIUMINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_INT = new FromInt32ToInt32(MDateAndTime.TIMESTAMP, MNumeric.INT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_INT = new FromInt32ToUnsignedInt32(MDateAndTime.TIMESTAMP, MNumeric.INT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_BIGINT = new FromInt32ToInt64(MDateAndTime.TIMESTAMP, MNumeric.BIGINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt32ToInt64(MDateAndTime.TIMESTAMP, MNumeric.BIGINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt32ToDouble(MDateAndTime.TIMESTAMP, MApproximateNumber.DOUBLE, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt32ToDecimal(MDateAndTime.TIMESTAMP, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DATE = new TCastBase(MDateAndTime.TIMESTAMP, MDateAndTime.DATE, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(MDateAndTime.encodeDate(MDateAndTime.decodeTimestamp(source.getInt32(),
                                                                                 context.getCurrentTimezone())));
        }
    };

    public static final TCast TO_TIME = new TCastBase(MDateAndTime.TIMESTAMP, MDateAndTime.TIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(MDateAndTime.encodeTime(MDateAndTime.decodeTimestamp(source.getInt32(),
                                                                                 context.getCurrentTimezone()), context));
        }
    };
    
    public static final TCast TO_DATETIME = new TCastBase(MDateAndTime.TIMESTAMP, MDateAndTime.DATETIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(MDateAndTime.encodeDateTime(MDateAndTime.decodeTimestamp(source.getInt32(),
                                                                                     context.getCurrentTimezone())));
        }
    };
}
