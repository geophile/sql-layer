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
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public final class MField extends TScalarBase {

    public static final TScalar[] exactOverloads = new TScalar[] {
            // From the MySQL docs:
            //
            //     If all arguments to FIELD() are strings, all arguments are compared as strings.
            //     If all arguments are numbers, they are compared as numbers.
            //     Otherwise, the arguments are compared as double.
            //
            // We can accomplish this by having three ScalarsGroups. The first consist of VARCHAR and CHAR. We want
            // more than one TScalar in this group, so that same-type-at is false and it's not automatically picked.
            // Only the various string types are strongly castable to either of these, so that takes care of the first
            // sentence in the FIELD spec (as quoted above). The second ScalarsGroup consists of exact numbers. We
            // provide these in four flavors: signed and unsigned of BIGINT and DECIMAL. This lets us cover all of the
            // exact precision types without expensive casts (casts between non-DECIMAL ints are cheap). Finally,
            // the DOUBLE overload is in a ScalarsGroup by itself, so that same-type-at is true and the scalar
            // is always picked.

            new MField(MString.VARCHAR, false, 1),
            new MField(MString.CHAR, false, 1),

            new MField(MNumeric.BIGINT, false, 2),
            new MField(MNumeric.BIGINT_UNSIGNED, false, 2),
            new MField(MNumeric.DECIMAL, false, 2),
            new MField(MNumeric.DECIMAL_UNSIGNED, false, 2),

            new MField(MApproximateNumber.DOUBLE, false, 3)
    };

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.setExact(exact).vararg(targetClass, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        ValueSource needle = inputs.get(0);
        int result = 0;
        if (!needle.isNull()) {
            TInstance needleInstance = needle.getType();
            for (int i = 1, size = inputs.size(); i < size; ++i) {
                ValueSource arg = inputs.get(i);
                if (!arg.isNull()) {
                    TInstance argInstance = arg.getType();
                    int cmp = TClass.compare(needleInstance, needle, argInstance, arg);
                    if (cmp == 0) {
                        result = i;
                        break;
                    }
                }
            }
        }
        output.putInt32(result);
    }

    @Override
    public String displayName() {
        return "FIELD";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.INT, 3);
    }

    @Override
    public int[] getPriorities() {
        return new int[] { priority };
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    private MField(TClass targetClass, boolean exact, int maxPriority) {
        this.targetClass = targetClass;
        this.exact = exact;
        this.priority = maxPriority;
    }

    private final TClass targetClass;
    private final int priority;
    private final boolean exact;
}
