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
package com.foundationdb.server.types.mcompat;

import com.foundationdb.server.error.InvalidDateFormatException;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.mcompat.mcasts.CastUtils;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.math.BigDecimal;

public class MParsers
{
    public static final TParser TINYINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt8((byte) CastUtils.parseInRange(source.getString(),
                                                         CastUtils.MAX_TINYINT, 
                                                         CastUtils.MIN_TINYINT,
                                                         context));
        }
    };
    
        
    public static final TParser UNSIGNED_TINYINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.parseInRange(source.getString(),
                                            CastUtils.MAX_UNSIGNED_TINYINT,
                                            0,
                                            context));
        }
    };
    
    public static final TParser SMALLINT = new TParser()
    {

        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.parseInRange(source.getString(),
                                             CastUtils.MAX_SMALLINT, 
                                             CastUtils.MIN_SMALLINT,
                                             context));
        }
    };

    public static final TParser UNSIGNED_SMALLINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.parseInRange(source.getString(),
                                          CastUtils.MAX_UNSIGNED_SMALLINT, 
                                          0,
                                          context));
        }
    };

    public static final TParser MEDIUMINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.parseInRange(source.getString(),
                                             CastUtils.MAX_MEDINT, 
                                             CastUtils.MIN_MEDINT,
                                             context));
        }
    };

    public static final TParser UNSIGNED_MEDIUMINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(CastUtils.parseInRange(source.getString(),
                                     CastUtils.MAX_UNSIGNED_MEDINT, 
                                     0,
                                     context));
        }
    };

    public static final TParser INT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.parseInRange(source.getString(),
                                           CastUtils.MAX_INT, 
                                           CastUtils.MIN_INT,
                                           context));
        }
    };

    public static final TParser UNSIGNED_INT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(CastUtils.parseInRange(source.getString(),
                                                   CastUtils.MAX_UNSIGNED_INT,
                                                   0,
                                                   context));
        }
    };

    public static final TParser BIGINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(CastUtils.parseInRange(source.getString(),
                                                   CastUtils.MAX_BIGINT, 
                                                   CastUtils.MIN_BIGINT,
                                                   context));
        }
    };

    public static final TParser UNSIGNED_BIGINT = new TParser() {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putInt64(CastUtils.parseUnsignedLong(source.getString(), context));
        }
    };
    
    public static final TParser FLOAT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putFloat((float)CastUtils.parseDoubleString(source.getString(), context));
        }
        
    };

    public static final TParser DOUBLE = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putDouble(CastUtils.parseDoubleString(source.getString(), context));
        }
    };
    
    public static final TParser DECIMAL = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            CastUtils.doCastDecimal(context,
                                    CastUtils.parseDecimalString(source.getString(),context),
                                    target);
        }   
    };

    public static final TParser DECIMAL_UNSIGNED = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            BigDecimalWrapperImpl wrapped = CastUtils.parseDecimalString(source.getString(), context);
            BigDecimal bd = wrapped.asBigDecimal();
            if (BigDecimal.ZERO.compareTo(bd) < 0)
                wrapped.reset();
            CastUtils.doCastDecimal(context, wrapped, target);
        }
    };
    
    public static final TParser DATE = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            try
            {
                int ret = MDateAndTime.parseAndEncodeDate(source.getString());
                target.putInt32(ret);
            }
            catch (InvalidDateFormatException e)
            {
                context.warnClient(e);
                target.putNull();
            }
        }
    };

    public static final TParser DATETIME = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            try
            {
                target.putInt64(MDateAndTime.parseAndEncodeDateTime(source.getString()));
            }
             catch (InvalidDateFormatException e)
            {
                context.warnClient(e);
                target.putNull();
            }
        }
    };
    
    public static final TParser TIME = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            try
            {
                target.putInt32(MDateAndTime.parseTime(source.getString(), context));
            }
            catch (InvalidDateFormatException e)
            {
                context.warnClient(e);
                target.putNull();
            }
        }
    };

    public static final TParser TIMESTAMP = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(MDateAndTime.parseAndEncodeTimestamp(source.getString(),
                                                                 context.getCurrentTimezone(),
                                                                 context));
        }
    };
    
    public static final TParser YEAR = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16(CastUtils.adjustYear(CastUtils.parseInRange(source.getString(),
                                                                        Long.MAX_VALUE,
                                                                        Long.MIN_VALUE,
                                                                        context),
                                                 context));
        }
    };
}
