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
package com.foundationdb.sql.optimizer;

import java.util.List;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

/** Update a row by substituting expressions for some fields. */
public class ExpressionRowUpdateFunction implements UpdateFunction
{
    private final List<TPreparedExpression> pExpressions;
    private final RowType rowType;

    public ExpressionRowUpdateFunction(List<TPreparedExpression> pExpressions, RowType rowType) {
        this.pExpressions = pExpressions;
        this.rowType = rowType;
    }

    /* UpdateFunction */

    @Override
    public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
        OverlayingRow result = new OverlayingRow(original);
        int nfields = rowType.nFields();
        for (int i = 0; i < nfields; i++) {
            TPreparedExpression expression = pExpressions.get(i);
            if (expression != null) {
                TEvaluatableExpression evaluation = expression.build();
                evaluation.with(original);
                evaluation.with(context);
                evaluation.with(bindings);
                evaluation.evaluate();
                result.overlay(i, evaluation.resultValue());
            }
        }
        return result;
    }

    /* Object */

    @Override
    public String toString() {
        String exprs = pExpressions.toString();
        return getClass().getSimpleName() + "(" + exprs + ")";
    }

}
