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

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.TExpressionExplainer;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

public final class TNullExpression implements TPreparedExpression {

    public TNullExpression (TInstance type) {
        this.type = type.withNullable(true);
    }

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        TEvaluatableExpression eval = build();
        return new TPreptimeValue(type, eval.resultValue());
    }

    @Override
    public TInstance resultType() {
        return type;
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation(type);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.LITERAL, "Literal", context);
        ex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance("NULL"));
        return ex;
    }

    @Override
    public String toString() {
        return "Literal(NULL)";
    }
    
    @Override
    public boolean isLiteral() {
        return true;
    }

    private final TInstance type;

    private static class InnerEvaluation implements TEvaluatableExpression {
        @Override
        public ValueSource resultValue() {
            return valueSource;
        }

        @Override
        public void evaluate() {
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
        }

        @Override
        public void with(QueryBindings bindings) {
        }

        private InnerEvaluation(TInstance underlying) {
            this.valueSource = ValueSources.getNullSource(underlying);
        }

        private final ValueSource valueSource;
    }
}
