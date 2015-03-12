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

import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;

import static com.foundationdb.server.types.mcompat.mtypes.MNumeric.*;

public abstract class Cast_From_Date extends TCastBase
{
    /**
     * TODO:
     * 
     * TIME
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
    public static final TCast TO_TINYINT = new Cast_From_Date(TINYINT)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt8((byte) CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, val, context));
        }
    };
    
    public static final TCast TO_UNSIGNED_TINYINT = new Cast_From_Date(TINYINT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, val, context));
        }   
    };

    public static final TCast TO_SMALLINT = new Cast_From_Date(SMALLINT)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, Short.MIN_VALUE, val, context));
        }
    };

    public static final TCast TO_UNSIGNED_SMALINT = new Cast_From_Date(SMALLINT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, val, context));
        }
    };
    
    public static final TCast TO_MEDIUMINT = new Cast_From_Date(MEDIUMINT)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt32(val);
        }
    };

    public static final TCast TO_UNSIGNED_MEDIUMINT = new Cast_From_Date(MEDIUMINT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, val, context));
        }
    };

    public static final TCast TO_INT = new Cast_From_Date(INT)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt32(val);
        }
    };
    
    public static final TCast TO_UNSIGNED_INT = new Cast_From_Date(INT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, val, context));
        }
    };
    
    public static final TCast TO_BIGINT = new Cast_From_Date(BIGINT)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt64(val);
        }
    };
    
    public static final TCast TO_UNSIGNED_BIGINT = new Cast_From_Date(BIGINT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putInt64(val);
        }
    };
    
    public static final TCast TO_DOUBLE = new Cast_From_Date(MApproximateNumber.DOUBLE)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putDouble(val);
        }
    };
    
    public static final TCast TO_DECIMAL = new Cast_From_Date(MNumeric.DECIMAL)
    {
        @Override
        protected void putOut(int val, ValueTarget out, TExecutionContext context)
        {
            out.putObject(new BigDecimalWrapperImpl(val));
        }
    };

    public static final TCast TO_DATETIME = new TCastBase(MDateAndTime.DATE, MDateAndTime.DATETIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            long[] ymd = MDateAndTime.decodeDate(source.getInt32());
            long[] ymdHMS = new long[6];
            System.arraycopy(ymd, 0, ymdHMS, 0, 3);
            long asDate = MDateAndTime.encodeDateTime(ymdHMS);
            target.putInt64(asDate);
        }
    };
    
    public static final TCast TO_TIME = new TCastBase(MDateAndTime.DATE, MDateAndTime.TIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            // DATE doesn't have any TIME
            target.putInt32(0);
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MDateAndTime.DATE, MDateAndTime.TIMESTAMP, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(MDateAndTime.encodeTimestamp(MDateAndTime.decodeDate(source.getInt32()),
                                                         context.getCurrentTimezone(),
                                                         context));
        }
    };
    
    protected abstract void putOut(int val, ValueTarget out, TExecutionContext context);
    
    private Cast_From_Date(TClass targetType)
    {
        super(MDateAndTime.DATE, targetType);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
    {
        putOut(packYMD(MDateAndTime.decodeDate(source.getInt32())),
               target,
               context);
    }
    
    private static int packYMD(long ymd[])
    {
        return (int) (ymd[MDateAndTime.YEAR_INDEX] * 10000
                       + ymd[MDateAndTime.MONTH_INDEX] * 100
                       + ymd[MDateAndTime.DAY_INDEX]);
    }
}
