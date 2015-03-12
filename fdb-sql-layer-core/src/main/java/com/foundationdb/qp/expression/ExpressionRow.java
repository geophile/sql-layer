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
package com.foundationdb.qp.expression;

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

public class ExpressionRow extends AbstractRow
{
    private RowType rowType;
    private List<? extends TPreparedExpression> pExpressions;
    private List<TEvaluatableExpression> pEvaluations;

    public ExpressionRow(RowType rowType, QueryContext context, QueryBindings bindings, 
                         List<? extends TPreparedExpression> pExpressions) {
        this.rowType = rowType;
        this.pExpressions = pExpressions;
        assert pExpressions != null : "pExpressions can not be null";
        this.pEvaluations = new ArrayList<>(pExpressions.size());
        for (TPreparedExpression expression : pExpressions) {
            TEvaluatableExpression evaluation = expression.build();
            evaluation.with(context);
            evaluation.with(bindings);
            this.pEvaluations.add(evaluation);
        }
    }

    /* AbstractRow */

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource uncheckedValue(int i) {
        TEvaluatableExpression eval = pEvaluations.get(i);
        eval.evaluate();
        return eval.resultValue();
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();        
    }

    /* Object */

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getClass().getSimpleName());
        str.append('[');
        int nf = rowType().nFields();
        for (int i = 0; i < nf; i++) {
            if (i > 0) str.append(", ");
            Object expression = pExpressions.get(i);
            if (expression != null)
                str.append(expression);
        }
        str.append(']');
        return str.toString();
    }

   @Override
   public boolean isBindingsSensitive() {
       return true;
   }
}
