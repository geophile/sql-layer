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

public class Cast_From_Datetime
{
    public static final TCast TO_TINYINT = new FromInt64ToInt8(MDateAndTime.DATETIME, MNumeric.TINYINT, false, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_TINYINT = new FromInt64ToUnsignedInt8(MDateAndTime.DATETIME, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_SMALLINT = new FromInt64ToInt16(MDateAndTime.DATETIME, MNumeric.SMALLINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_SMALINT = new FromInt64ToUnsignedInt16(MDateAndTime.DATETIME, MNumeric.SMALLINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_MEDIUMINT = new FromInt64ToInt32(MDateAndTime.DATETIME, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt64ToUnsignedInt32(MDateAndTime.DATETIME, MNumeric.MEDIUMINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_INT = new FromInt64ToInt32(MDateAndTime.DATETIME, MNumeric.INT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_INT = new FromInt64ToUnsignedInt32(MDateAndTime.DATETIME, MNumeric.INT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_BIGINT = new FromInt64ToInt64(MDateAndTime.DATETIME, MNumeric.BIGINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt64ToInt64(MDateAndTime.DATETIME, MNumeric.BIGINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt64ToDouble(MDateAndTime.DATETIME, MApproximateNumber.DOUBLE, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt64ToDecimal(MDateAndTime.DATETIME, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DATE = new TCastBase(MDateAndTime.DATETIME, MDateAndTime.DATE, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(MDateAndTime.encodeDate(MDateAndTime.decodeDateTime(source.getInt64())));
        }
    };
    
    public static final TCast TO_TIME = new TCastBase(MDateAndTime.DATETIME, MDateAndTime.TIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(MDateAndTime.encodeTime(MDateAndTime.decodeDateTime(source.getInt64()), context));
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MDateAndTime.DATETIME, MDateAndTime.TIMESTAMP, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(MDateAndTime.encodeTimestamp(MDateAndTime.decodeDateTime(source.getInt64()),
                                                         context.getCurrentTimezone(),
                                                         context));
        }
    };
}
