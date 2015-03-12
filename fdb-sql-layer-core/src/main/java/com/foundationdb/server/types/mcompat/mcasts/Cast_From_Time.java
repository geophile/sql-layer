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

import com.foundationdb.server.types.*;
import static com.foundationdb.server.types.mcompat.mcasts.MNumericCastBase.*;

import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;

public class Cast_From_Time {

   /**
     * TODO:
     * 
     * YEAR
     * TIMESTAMP
     * 
     * BIT
     * CHAR
     * BINARY
     * VARBINARY
     * TINYBLOG
     * TINYTEXT
     * TEXT
     * MEDIUMBLOB
     * MEDIUMTEXT
     * LONGBLOG
     * LONTTEXT
     * 
     */
    
    public static final TCast TO_TINYINT = new FromInt32ToInt8(MDateAndTime.TIME, MNumeric.TINYINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_TINYINT = new FromInt32ToUnsignedInt8(MDateAndTime.TIME, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN);

    public static final TCast TO_SMALLINT = new FromInt32ToInt16(MDateAndTime.TIME, MNumeric.SMALLINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_SMALLINT = new FromInt32ToUnsignedInt16(MDateAndTime.TIME, MNumeric.SMALLINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_MEDIUMINT = new FromInt32ToInt32(MDateAndTime.TIME, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt32ToUnsignedInt32(MDateAndTime.TIME, MNumeric.MEDIUMINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_INT = new FromInt32ToInt32(MDateAndTime.TIME, MNumeric.INT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_INT = new FromInt32ToUnsignedInt32(MDateAndTime.TIME, MNumeric.INT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_BIGINT = new FromInt32ToInt64(MDateAndTime.TIME, MNumeric.BIGINT, false, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_BIGINT = new FromInt32ToInt64(MDateAndTime.TIME, MNumeric.BIGINT_UNSIGNED, false, Constantness.UNKNOWN);

    public static final TCast TO_DOUBLE = new FromInt32ToDouble(MDateAndTime.TIME, MApproximateNumber.DOUBLE, false, Constantness.UNKNOWN);

    public static final TCast TO_DECIMAL = new FromInt32ToDecimal(MDateAndTime.TIME, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    // Generally, TIME cannot be converted to DATE, DATETIME or TIMESTAMP
    // Any cast from <TIME> --> <DATE> | <DATETIME> | <TIMESTAMP> should result in zeros
    // and a warning.
    // But contrast the similarly named _functions_, which do not give a warning and
    // do preserve the time fields when appropriate.
    public static final TCast TO_DATETIME = new TCastBase(MDateAndTime.TIME, MDateAndTime.DATETIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            // direct cast TIME --> DATETIME results in a truncation to zero
            context.reportTruncate(String.valueOf(source.getInt32()), "0000-00-00 00:00:00");
            target.putInt64(0);
        }
    };
    
    public static final TCast TO_DATE = new TCastBase(MDateAndTime.TIME, MDateAndTime.DATE, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            // direct cast TIME --> DATE results in a truncation to zero
            context.reportTruncate(String.valueOf(source.getInt32()), "0000-00-00");
            target.putInt32(0);
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MDateAndTime.TIME, MDateAndTime.TIMESTAMP, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            // direct cast TIME --> TIMESTAMP results in a truncation to zero
            context.reportTruncate(String.valueOf(source.getInt32()), "0000-00-00 00:00:00");
            target.putInt32(0);
        }
    };
}
