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
package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

/**
 * <p>A function for describing the prepare-time type information of other expression. If/when we divide bundles into
 * modular packages, this should go into a testing or debug package. Its primary purpose, at least for now, is to verify
 * in our yaml tests that expressions have the right type.</p>
 *
 * <p>The usage is <code>DESCRIBE_EXPRESSION(<i>expr</i>)</code>, and the result is a constant {@code VARCHAR(255)} which
 * describes the TInstance and constantness of <i>expr</i>.</p>
 */
public final class DescribeExpression extends TScalarBase {

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(null, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        ValueSource input = inputs.get(0);
        String result = input.getType().toString();
        result = ((input.isNull()) ? "variable " : "const ") + result;
        output.putString(result, null);
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    protected Constantness constness(TPreptimeContext context, int inputIndex, LazyList<? extends TPreptimeValue> values) {
        return Constantness.CONST;
    }

    @Override
    protected boolean allowNonConstsInEvaluation() {
        return true;
    }

    @Override
    public String displayName() {
        return "describe_expression";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(varchar, 255);
    }

    private final TClass varchar;

    public DescribeExpression(TClass varchar) {
        this.varchar = varchar;
    }
}
