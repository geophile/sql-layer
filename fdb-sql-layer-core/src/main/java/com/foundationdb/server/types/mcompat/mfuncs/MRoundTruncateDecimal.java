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

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MRoundTruncateDecimal extends TScalarBase {

    public static final Collection<TScalar> overloads = createAll();

    private static final int DEC_INDEX = 0;

    private enum RoundingStrategy {
        ROUND {
            @Override
            protected void apply(BigDecimalWrapper io, int scale) {
                io.round(scale);
            }
        },
        TRUNCATE {
            @Override
            protected void apply(BigDecimalWrapper io, int scale) {
                io.truncate(scale);
            }
        };

        protected abstract void apply(BigDecimalWrapper io, int scale);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        BigDecimalWrapper result = TBigDecimal.getWrapper(context, DEC_INDEX);
        ValueSource input = inputs.get(0);
        result.set(TBigDecimal.getWrapper(input, input.getType()));
        int scale = signatureStrategy.roundToScale(inputs);
        roundingStrategy.apply(result, scale);
        output.putObject(result);
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TPreptimeValue valueToRound = inputs.get(0);
                ValueSource roundToPVal = signatureStrategy.getScaleOperand(inputs);
                int precision, scale;
                if ((roundToPVal == null) || roundToPVal.isNull()) {
                    precision = 17;
                    int incomingScale = valueToRound.type().attribute(DecimalAttribute.SCALE);
                    if (incomingScale > 1)
                        precision += (incomingScale - 1);
                    scale = incomingScale;
                } else {
                    scale = roundToPVal.getInt32();

                    TInstance incomingInstance = valueToRound.type();
                    int incomingPrecision = incomingInstance.attribute(DecimalAttribute.PRECISION);
                    int incomingScale = incomingInstance.attribute(DecimalAttribute.SCALE);

                    precision = incomingPrecision;
                    if (incomingScale > 1)
                        precision -= (incomingScale - 1);
                    precision += scale;

                }
                return MNumeric.DECIMAL.instance(precision, scale, anyContaminatingNulls(inputs));
            }
        });
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        signatureStrategy.buildInputSets(MNumeric.DECIMAL, builder);
    }

    @Override
    public String displayName() {
        return roundingStrategy.name();
    }

    protected MRoundTruncateDecimal(RoundingOverloadSignature signatureStrategy,
                                    RoundingStrategy roundingStrategy)
    {
        this.signatureStrategy = signatureStrategy;
        this.roundingStrategy = roundingStrategy;
    }

    private static Collection<TScalar> createAll() {
        List<TScalar> results = new ArrayList<>();
        for (RoundingOverloadSignature signature : RoundingOverloadSignature.values()) {
            for (RoundingStrategy rounding : RoundingStrategy.values()) {
                results.add(new MRoundTruncateDecimal(signature, rounding));
            }
        }
        return Collections.unmodifiableCollection(results);
    }

    private final RoundingOverloadSignature signatureStrategy;
    private final RoundingStrategy roundingStrategy;
}
