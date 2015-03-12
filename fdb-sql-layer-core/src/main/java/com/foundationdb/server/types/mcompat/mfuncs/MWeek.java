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
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
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
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

/**
 * Implements WEEK(<date>[,mode]) and WEEKOFYEAR(<date>)
 */
public abstract class MWeek extends TScalarBase
{
    public static final TScalar INSTANCES[] = new TScalar[]
    {
        new MWeek("WEEK")
        {
            @Override
            protected int getMode(TExecutionContext context, LazyList<? extends ValueSource> inputs)
            {
                return 0;
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDateAndTime.DATE, 0);
            }
        },
        new MWeek("WEEKOFYEAR")
        {
            @Override
            protected int getMode(TExecutionContext context, LazyList<? extends ValueSource> inputs)
            {
                return 3;
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDateAndTime.DATE, 0);
            }
        },
        new MWeek("WEEK")
        {
            @Override
            protected int getMode(TExecutionContext context, LazyList<? extends ValueSource> inputs)
            {
                int mode = (int)inputs.get(1).getInt64();
                if (mode < 0 || mode > 7)
                {
                    context.warnClient(new InvalidParameterValueException("MODE out of range [0, 7]: " + mode));
                    return -1;
                }
                return mode;
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDateAndTime.DATE, 0).covers(MNumeric.BIGINT, 1);
            }
        }
    };

    protected abstract int getMode(TExecutionContext context, LazyList<? extends ValueSource> inputs);
    
    private final String name;
    private MWeek(String name)
    {
        this.name = name;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        int date = inputs.get(0).getInt32();
        long ymd[] = MDateAndTime.decodeDate(date);
        int mode = getMode(context, inputs);
        if (!MDateAndTime.isValidDateTime(ymd, ZeroFlag.YEAR) || mode < 0)
        {
            context.warnClient(new InvalidDateFormatException("date", Integer.toString(date)));
            output.putNull();
        }
        else
        {
            output.putInt32(modes[(int) mode].getWeek(new MutableDateTime(DateTimeZone.forID(context.getCurrentTimezone())),
                                                  (int)ymd[0], (int)ymd[1], (int)ymd[2]));
        }
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT);
    }

    //------------ static helper------------------------------------------------
    private static interface Modes
    {
        int getWeek(MutableDateTime cal, int yr, int mo, int da);
    }

    private static final Modes[] modes = new Modes[]
    {
      new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DateTimeConstants.SUNDAY, 8);}}, //0
      new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DateTimeConstants.SUNDAY,8);}},  //1
      new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DateTimeConstants.SUNDAY, 0);}}, //2
      new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DateTimeConstants.SUNDAY, 1);}}, //3
      new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DateTimeConstants.SATURDAY,8);}},//4
      new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DateTimeConstants.MONDAY, 8);}}, //5
      new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DateTimeConstants.SATURDAY,4);}},//6
      new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DateTimeConstants.MONDAY,5);}},  //7
      new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return 0;}} // dummy always return 0-lowestval
    };

    private static int getMode1346(MutableDateTime cal, int yr, int mo, int da, int firstDay, int lowestVal)
    {
        cal.setYear(yr);
        cal.setMonthOfYear(1);
        cal.setDayOfMonth(1);

        int firstD = 1;

        while (cal.getDayOfWeek() != firstDay) 
            cal.setDayOfMonth(++firstD);

        cal.setYear(yr);
        cal.setMonthOfYear(mo);
        cal.setDayOfMonth(da);

        int week = cal.getDayOfYear() - (firstD +1 ); // Sun/Mon
        if (firstD < 4)
        {
            if (week < 0) return modes[lowestVal].getWeek(cal, yr-1, 12, 31);
            else return week / 7 + 1;
        }
        else
        {
            if (week < 0) return 1;
            else return week / 7 + 2;
        }
    }

    private static int getMode0257(MutableDateTime cal, int yr, int mo, int da, int firstDay, int lowestVal)
    {
        cal.setYear(yr);
        cal.setMonthOfYear(1);
        cal.setDayOfMonth(1);
        int firstD = 1;

        while (cal.getDayOfWeek() != firstDay)
            cal.setDayOfMonth(++firstD);

        cal.setYear(yr); 
        cal.setMonthOfYear(mo); 
        cal.setDayOfMonth(da); 

        int dayOfYear = cal.getDayOfYear(); 

        if (dayOfYear < firstD) return modes[lowestVal].getWeek(cal, yr-1, 12, 31);
        else return (dayOfYear - firstD) / 7 +1;
    }       

}
