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

import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

@SuppressWarnings("unused") // Used by reflected
public class Cast_From_Boolean {

    public static final TCast BOOL_TO_INTEGER = new TCastBase(AkBool.INSTANCE, MNumeric.INT) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putInt32(source.getBoolean() ? 1 : 0);
        }
    };

    // MApproximateNumber
    public static final TCast FLOAT_TO_BOOLEAN = new TCastBase(MApproximateNumber.FLOAT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getFloat() != 0.0);
        }
    };
    public static final TCast FLOAT_UNSIGNED_TO_BOOLEAN = new TCastBase(MApproximateNumber.FLOAT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getFloat() != 0.0);
        }
    };
    public static final TCast DOUBLE_TO_BOOLEAN = new TCastBase(MApproximateNumber.DOUBLE, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getDouble() != 0.0);
        }
    };
    public static final TCast DOUBLE_UNSIGNED_TO_BOOLEAN = new TCastBase(MApproximateNumber.DOUBLE_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getDouble() != 0.0);
        }
    };


    // MBinary
    // TODO

    // MDatetimesfinal
    public static final TCast DATE_TO_BOOLEAN = new TCastBase(MDateAndTime.DATE, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast DATETIME_TO_BOOLEAN = new TCastBase(MDateAndTime.DATETIME, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast TIME_TO_BOOLEAN = new TCastBase(MDateAndTime.TIME, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast YEAR_TO_BOOLEAN = new TCastBase(MDateAndTime.YEAR, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt16() != 0);
        }
    };
    public static final TCast TIMESTAMP_TO_BOOLEAN = new TCastBase(MDateAndTime.TIMESTAMP, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };

    // MNumeric
    public static final TCast TINYINT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.TINYINT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt16() != 0);
        }
    };
    public static final TCast SMALLINT_TO_BOOLEAN = new TCastBase(MNumeric.SMALLINT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt16() != 0);
        }
    };
    public static final TCast SMALLINT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.SMALLINT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast MEDIUMINT_TO_BOOLEAN = new TCastBase(MNumeric.MEDIUMINT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast MEDIUMINT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.MEDIUMINT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast INT_TO_BOOLEAN = new TCastBase(MNumeric.INT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast INT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.INT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast BIGINT_TO_BOOLEAN = new TCastBase(MNumeric.BIGINT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast BIGINT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.BIGINT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast DECIMAL_TO_BOOLEAN = new TCastBase(MNumeric.DECIMAL, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            BigDecimalWrapper decimal = TBigDecimal.getWrapper(source, source.getType());
            target.putBool(!decimal.isZero());
        }
    };
    public static final TCast DECIMAL_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.DECIMAL_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            BigDecimalWrapper decimal = TBigDecimal.getWrapper(source, source.getType());
            target.putBool(!decimal.isZero());
        }
    };

    // MString
    // Handled by string->boolean (i.e. MParsers.BOOLEAN)
}
