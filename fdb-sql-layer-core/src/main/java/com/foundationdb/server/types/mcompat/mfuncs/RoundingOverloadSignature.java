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
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;

import java.util.List;

enum RoundingOverloadSignature {
    ONE_ARG {
        @Override
        protected int roundToScale(LazyList<? extends ValueSource> inputs) {
            return 0;
        }

        @Override
        protected void buildInputSets(TClass arg0, TInputSetBuilder builder) {
            builder.covers(arg0, 0);
        }

        @Override
        protected ValueSource getScaleOperand(List<? extends TPreptimeValue> inputs) {
            return ZERO;
        }

        private final ValueSource ZERO = new Value(MNumeric.INT.instance(false), 0);
    },
    TWO_ARGS {
        @Override
        protected int roundToScale(LazyList<? extends ValueSource> inputs) {
            return inputs.get(1).getInt32();
        }

        @Override
        protected void buildInputSets(TClass arg0, TInputSetBuilder builder) {
            builder.covers(arg0, 0).covers(MNumeric.INT, 1);
        }

        @Override
        protected ValueSource getScaleOperand(List<? extends TPreptimeValue> inputs) {
            return inputs.get(1).value();
        }
    };

    protected abstract int roundToScale(LazyList<? extends ValueSource> inputs);
    protected abstract void buildInputSets(TClass arg0, TInputSetBuilder builder);
    protected abstract ValueSource getScaleOperand(List<? extends TPreptimeValue> inputs);
}
