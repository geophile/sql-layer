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

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.funcs.Abs;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

public class MAbs {

    private static final int DEC_INDEX = 0;

    public static final TScalar TINYINT = new Abs(MNumeric.TINYINT) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            output.putInt8((byte) Math.abs(inputs.get(0).getInt8()));
        }
    };
    public static final TScalar SMALLINT = new Abs(MNumeric.SMALLINT) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            output.putInt16((short) Math.abs(inputs.get(0).getInt16()));
        }
    };
    public static final TScalar MEDIUMINT = new Abs(MNumeric.MEDIUMINT) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            output.putInt32((int) Math.abs(inputs.get(0).getInt32()));
        }
    };
    public static final TScalar BIGINT = new Abs(MNumeric.BIGINT) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            output.putInt64((long) Math.abs(inputs.get(0).getInt64()));
        }
    };
    public static final TScalar INT = new Abs(MNumeric.INT) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            output.putInt32((int) Math.abs(inputs.get(0).getInt32()));
        }
    };
    public static final TScalar DECIMAL = new Abs(MNumeric.DECIMAL) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            ValueSource input = inputs.get(0);
            BigDecimalWrapper wrapper =
                TBigDecimal.getWrapper(context, DEC_INDEX)
                .set(TBigDecimal.getWrapper(input, input.getType()));
            output.putObject(wrapper.abs());
        }

        @Override
        protected void buildInputSets(TInputSetBuilder builder)
        {
            builder.pickingCovers(MNumeric.DECIMAL, 0);
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.picking();
        }
    };
    public static final TScalar DOUBLE = new Abs(MApproximateNumber.DOUBLE) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            output.putDouble(Math.abs(inputs.get(0).getDouble()));
        }
    };
}
