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
import com.foundationdb.server.error.SubqueryTooManyRowsException;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;

public class ScalarSubqueryTExpression extends SubqueryTExpression
{
    private static final class InnerEvaluation extends SubqueryTEvaluateble
    {
        private final TPreparedExpression expression;
        
        public InnerEvaluation(Operator subquery,
                               TPreparedExpression expression,
                               RowType outerRowType, RowType innerRowType,
                               int bindingPosition)
        {
            super(subquery, outerRowType, innerRowType, bindingPosition, expression.resultType());
            this.expression = expression;
        }

        @Override
        protected void doEval(ValueTarget out)
        {
            Row row = next();
            if (row == null)
                out.putNull();
            else
            {
                TEvaluatableExpression eval = expression.build();
                
                eval.with(queryContext());
                eval.with(queryBindings());
                eval.with(row);

                // evaluate the result
                eval.evaluate();
                ValueTargets.copyFrom(eval.resultValue(), out);
                
                if (next() != null)
                    throw new SubqueryTooManyRowsException();
            }
        }
    }

    private final TPreparedExpression expression;
    
    public ScalarSubqueryTExpression(Operator subquery,
                                     TPreparedExpression expression,
                                     RowType outerRowType, RowType innerRowType, 
                                     int bindingPosition)
    {
        super(subquery, outerRowType, innerRowType, bindingPosition);
        this.expression = expression;
    }

    @Override
    public TInstance resultType()
    {
        return expression.resultType();
    }

    @Override
    public TEvaluatableExpression build()
    {
        return new InnerEvaluation(subquery(),
                                   expression,
                                   outerRowType(), innerRowType(),
                                   bindingPosition());
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("VALUE"));
        explainer.addAttribute(Label.EXPRESSIONS, expression.getExplainer(context));
        return explainer;
    }
}
