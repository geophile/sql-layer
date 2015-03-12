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
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.DoubleAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MRoundTruncateDouble extends TScalarBase {

    public static final Collection<TScalar> overloads = createAll();

    private enum RoundingStrategy {
        ROUND(RoundingMode.HALF_UP),
        TRUNCATE(RoundingMode.DOWN)
        ;

        public double apply(double input, int scale) {
            BigDecimal asDecimal = BigDecimal.valueOf(input);
            asDecimal = asDecimal.setScale(scale, roundingMode);
            return asDecimal.doubleValue();
        }

        private RoundingStrategy(RoundingMode roundingMode) {
            this.roundingMode = roundingMode;
        }

        private final RoundingMode roundingMode;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        double input = inputs.get(0).getDouble();
        int scale = signatureStrategy.roundToScale(inputs);
        double result = roundingStrategy.apply(input, scale);
        output.putDouble(result);
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                ValueSource incomingScale = signatureStrategy.getScaleOperand(inputs);
                int resultScale = (incomingScale == null)
                        ? inputs.get(0).type().attribute(DoubleAttribute.SCALE)
                        : incomingScale.getInt32();
                int resultPrecision = 17 + resultScale;
                return MApproximateNumber.DOUBLE.instance(resultPrecision, resultScale, anyContaminatingNulls(inputs));
            }
        });
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        signatureStrategy.buildInputSets(MApproximateNumber.DOUBLE, builder);
    }

    @Override
    public String displayName() {
        return roundingStrategy.name();
    }

    protected MRoundTruncateDouble(RoundingOverloadSignature signatureStrategy,
                                   RoundingStrategy roundingStrategy)
    {
        this.signatureStrategy = signatureStrategy;
        this.roundingStrategy = roundingStrategy;
    }

    private static Collection<TScalar> createAll() {
        List<TScalar> results = new ArrayList<>();
        for (RoundingOverloadSignature signature : RoundingOverloadSignature.values()) {
            for (RoundingStrategy rounding : RoundingStrategy.values()) {
                results.add(new MRoundTruncateDouble(signature, rounding));
            }
        }
        return Collections.unmodifiableCollection(results);
    }

    private final RoundingOverloadSignature signatureStrategy;
    private final RoundingStrategy roundingStrategy;
}
