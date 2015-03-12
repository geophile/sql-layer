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
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.math.BigDecimal;

public final class Cast_From_Decimal {

    public static final TCast TO_DECIMAL_UNSIGNED = new TCastBase(MNumeric.DECIMAL, MNumeric.DECIMAL_UNSIGNED) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            TBigDecimal.adjustAttrsAsNeeded(context, source, context.outputType(), target);
        }
    };

    public static final TCast TO_DECIMAL = new TCastBase(MNumeric.DECIMAL_UNSIGNED, MNumeric.DECIMAL) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            TBigDecimal.adjustAttrsAsNeeded(context, source, context.outputType(), target);
        }
    };

    public static final TCast TO_FLOAT = new TCastBase(MNumeric.DECIMAL, MApproximateNumber.FLOAT) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            BigDecimalWrapper decimal = TBigDecimal.getWrapper(source, source.getType());
            float asFloat = decimal.asBigDecimal().floatValue();
            target.putFloat(asFloat);
        }
    };

    public static final TCast TO_DOUBLE = new TCastBase(MNumeric.DECIMAL, MApproximateNumber.DOUBLE) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            BigDecimalWrapper decimal = TBigDecimal.getWrapper(source, source.getType());
            double asDouble = decimal.asBigDecimal().doubleValue();
            target.putDouble(asDouble);
        }
    };

    public static final TCast TO_BIGINT = new TCastBase(MNumeric.DECIMAL, MNumeric.BIGINT) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            BigDecimalWrapper wrapped = TBigDecimal.getWrapper(source, source.getType());
            BigDecimal bd = wrapped.asBigDecimal();
            int signum = bd.signum();
            long longVal;
            if ( (signum < 0) && (bd.compareTo(LONG_MINVAL) < 0)) {
                context.reportTruncate(bd.toString(), Long.toString(LONG_MIN_TRUNCATE));
                longVal = LONG_MIN_TRUNCATE;
            }
            else if ( (signum > 0) && (bd.compareTo(LONG_MAXVAL) > 0)) {
                context.reportTruncate(bd.toString(), Long.toString(LONG_MAX_TRUNCATE));
                longVal = LONG_MAX_TRUNCATE;
            }
            else {
                // TODO make sure no loss of precision causes an error
                longVal = bd.longValue();
            }
            target.putInt64(longVal);
        }
    };

    private Cast_From_Decimal() {}

    private static BigDecimal LONG_MAXVAL = BigDecimal.valueOf(Long.MAX_VALUE);
    private static BigDecimal LONG_MINVAL = BigDecimal.valueOf(Long.MIN_VALUE);
    private static long LONG_MAX_TRUNCATE = Long.MAX_VALUE;
    private static long LONG_MIN_TRUNCATE = Long.MIN_VALUE;
}
