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

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.value.ValueTarget;

public final class AnySubqueryTExpression extends SubqueryTExpression {

    @Override
    public TInstance resultType() {
        return AkBool.INSTANCE.instance(true);
    }

    @Override
    public TEvaluatableExpression build() {
        TEvaluatableExpression child = expression.build();
        return new InnerEvaluatable(subquery(), child, outerRowType(), innerRowType(), bindingPosition());
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("ANY"));
        explainer.addAttribute(Label.EXPRESSIONS, expression.getExplainer(context));
        return explainer;
    }

    public AnySubqueryTExpression(Operator subquery, TPreparedExpression expression,
                                  RowType outerRowType, RowType innerRowType, int bindingPosition)
    {
        super(subquery, outerRowType, innerRowType, bindingPosition);
        this.expression = expression;
    }

    private final TPreparedExpression expression;

    private static class InnerEvaluatable extends SubqueryTEvaluateble {

        @Override
        protected void doEval(ValueTarget out) {
            evaluation.with(queryContext());
            evaluation.with(queryBindings());
            Boolean result = Boolean.FALSE;
            while (true) {
                Row row = next();
                if (row == null) break;
                evaluation.with(row);
                evaluation.evaluate();
                if (evaluation.resultValue().isNull()) {
                    result = null;
                }
                else if (evaluation.resultValue().getBoolean()) {
                    result = Boolean.TRUE;
                    break;
                }
            }
            if (result == null)
                out.putNull();
            else
                out.putBool(result == Boolean.TRUE);

        }

        private InnerEvaluatable(Operator subquery, TEvaluatableExpression evaluation, RowType outerRowType,
                                       RowType innerRowType, int bindingPosition)
        {
            super(subquery, outerRowType, innerRowType, bindingPosition, AkBool.INSTANCE.instance(true));
            this.evaluation = evaluation;
        }

        private final TEvaluatableExpression evaluation;
    }
}
