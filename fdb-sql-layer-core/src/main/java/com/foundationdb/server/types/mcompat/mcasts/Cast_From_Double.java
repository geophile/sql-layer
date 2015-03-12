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
import com.foundationdb.server.types.TCastPath;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

public final class Cast_From_Double {

    public static final TCast FLOAT_TO_DOUBLE = new TCastBase(MApproximateNumber.FLOAT, MApproximateNumber.DOUBLE) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putDouble(source.getFloat());
        }
    };

    public static final TCast FLOAT_UNSIGNED_TO_DOUBLE = new TCastBase(MApproximateNumber.FLOAT_UNSIGNED, MApproximateNumber.DOUBLE) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putDouble(source.getFloat());
        }
    };

    public static final TCast DOUBLE_UNSIGNED_TO_DOUBLE = new TCastBase(MApproximateNumber.DOUBLE_UNSIGNED, MApproximateNumber.DOUBLE) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putDouble(source.getDouble());
        }
    };

    public static final TCastPath FLOAT_TO_VARCHAR = TCastPath.create(
            MApproximateNumber.FLOAT,
            MApproximateNumber.DOUBLE,
            MString.VARCHAR
    );

    public static final TCastPath FLOAT_UNSIGNED_TO_VARCHAR = TCastPath.create(
            MApproximateNumber.FLOAT_UNSIGNED,
            MApproximateNumber.DOUBLE,
            MString.VARCHAR
    );

    public static final TCastPath DOUBLE_UNSIGNED_TO_VARCHAR = TCastPath.create(
            MApproximateNumber.DOUBLE_UNSIGNED,
            MApproximateNumber.DOUBLE,
            MString.VARCHAR
    );

    public static final TCast TO_FLOAT = new TCastBase(MApproximateNumber.DOUBLE, MApproximateNumber.FLOAT) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            double orig = source.getDouble();
            double positive = Math.abs(orig);
            if (positive > Float.MAX_VALUE) {
                context.reportTruncate(Double.toString(orig), Float.toString(Float.MAX_VALUE));
                orig = Float.MAX_VALUE; // TODO or is it null?
            }
            target.putFloat((float)orig);
        }
    };

    public static final TCast TO_FLOAT_UNSIGNED = new TCastBase(MApproximateNumber.DOUBLE, MApproximateNumber.FLOAT_UNSIGNED) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            double orig = source.getDouble();
            if (orig < 0) {
                context.reportTruncate(Double.toString(orig), "0");
                orig = 0; // TODO or is it null?
            }
            else if (orig > Float.MAX_VALUE) {
                context.reportTruncate(Double.toString(orig), Float.toString(Float.MAX_VALUE));
                orig = Float.MAX_VALUE; // TODO or is it null?
            }
            target.putFloat((float)orig);
        }
    };

    public static final TCast TO_DOUBLE_UNSIGNED = new TCastBase(MApproximateNumber.DOUBLE, MApproximateNumber.DOUBLE_UNSIGNED) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            double orig = source.getDouble();
            if (orig < 0) {
                context.reportTruncate(Double.toString(orig), "0");
                orig = 0; // TODO or is it null?
            }
            target.putDouble(orig);
        }
    };
}
