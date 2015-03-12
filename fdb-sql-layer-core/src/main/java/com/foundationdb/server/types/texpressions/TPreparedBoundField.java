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
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;

public final class TPreparedBoundField implements TPreparedExpression {
    
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return new TPreptimeValue(resultType());
    }

    @Override
    public TInstance resultType() {
        return fieldExpression.resultType();
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation(fieldExpression.build(), rowPosition);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        // Extend Field inside, rather than wrapping it.
        CompoundExplainer ex = fieldExpression.getExplainer(context);
        ex.get().remove(Label.NAME); // Want to replace.
        ex.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("Bound"));
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(rowPosition));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }

    @Override
    public String toString() {
        return "Bound(" + rowPosition + ',' + fieldExpression + ')';
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    public TPreparedBoundField(RowType rowType, int rowPosition, int fieldPosition) {
        fieldExpression = new TPreparedField(rowType.typeAt(fieldPosition), fieldPosition);
        this.rowPosition = rowPosition;
    }

    private final TPreparedField fieldExpression;
    private final int rowPosition;

    private static class InnerEvaluation implements TEvaluatableExpression {

        @Override
        public ValueSource resultValue() {
            return fieldEvaluation.resultValue();
        }

        @Override
        public void evaluate() {
            fieldEvaluation.evaluate();
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
            fieldEvaluation.with(context);
        }

        @Override
        public void with(QueryBindings bindings) {
            fieldEvaluation.with(bindings.getRow(rowPosition));
        }

        private InnerEvaluation(TEvaluatableExpression fieldEvaluation, int rowPosition) {
            this.fieldEvaluation = fieldEvaluation;
            this.rowPosition = rowPosition;
        }

        private final TEvaluatableExpression fieldEvaluation;
        private final int rowPosition;
    }
}
