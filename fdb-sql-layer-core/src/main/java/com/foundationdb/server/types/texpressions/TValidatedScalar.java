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
package com.foundationdb.server.types.texpressions;

import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.List;

public final class TValidatedScalar extends TValidatedOverload implements TScalar {

    // TOverload methods (straight delegation)

    @Override
    public TPreptimeValue evaluateConstant(TPreptimeContext context, LazyList<? extends TPreptimeValue> inputs) {
        return scalar.evaluateConstant(context, inputs);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        scalar.finishPreptimePhase(context);
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        scalar.evaluate(context, inputs, output);
    }

    @Override
    public String toString(List<? extends TPreparedExpression> inputs, TInstance resultType) {
        return scalar.toString(inputs, resultType);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context, List<? extends TPreparedExpression> inputs, TInstance resultType)
    {
        return scalar.getExplainer(context, inputs, resultType);
    }

    public TValidatedScalar(TScalar scalar) {
        this(scalar, scalar.inputSets());
    }

    private TValidatedScalar(TScalar scalar, List<TInputSet> inputSets) {
        super(scalar, inputSets);
        this.scalar = scalar;
    }

    private final TScalar scalar;
}
