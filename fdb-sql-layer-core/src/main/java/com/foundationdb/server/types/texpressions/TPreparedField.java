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

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.TExpressionExplainer;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;

public final class TPreparedField implements TPreparedExpression {
    
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return null;
    }

    @Override
    public TInstance resultType() {
        return typeInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new Evaluation(typeInstance, fieldIndex);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.FIELD, "Field", context);
        ex.addAttribute(Label.POSITION, PrimitiveExplainer.getInstance(fieldIndex));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }

    @Override
    public String toString() {
        return "Field(" + fieldIndex + ')';
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    public TPreparedField(TInstance typeInstance, int fieldIndex) {
        this.typeInstance = typeInstance;
        this.fieldIndex = fieldIndex;
    }

    private final TInstance typeInstance;
    private final int fieldIndex;
    
    private static class Evaluation extends ContextualEvaluation<Row> {
        @Override
        protected void evaluate(Row context, ValueTarget target) {
            ValueSource rowSource = context.value(fieldIndex);
            ValueTargets.copyFrom(rowSource, target);
        }

        @Override
        public void with(Row row) {
            setContext(row);
        }

        private Evaluation(TInstance underlyingType, int fieldIndex) {
            super(underlyingType);
            this.fieldIndex = fieldIndex;
        }

        private int fieldIndex;
    }
}
