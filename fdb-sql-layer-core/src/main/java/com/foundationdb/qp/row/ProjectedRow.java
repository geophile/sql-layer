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
package com.foundationdb.qp.row;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.ProjectedRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.AkibanAppender;

import java.util.ArrayList;
import java.util.List;

public class ProjectedRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(buffer);
        buffer.append('(');
        boolean first = true;
        for (int i = 0, pEvalsSize = pEvaluatableExpressions.size(); i < pEvalsSize; i++) {
            ValueSource evaluation = value(i);
            TInstance type = rowType.typeAt(i);
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            if (type != null) {
                type.format(evaluation, appender);
            } else {
                buffer.append("NULL");
            }
        }
        buffer.append(')');
        return buffer.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource uncheckedValue(int index) {
        TEvaluatableExpression evaluatableExpression = pEvaluatableExpressions.get(index);
        if (!evaluated[index]) {
            evaluatableExpression.with(context);
            evaluatableExpression.with(bindings);
            evaluatableExpression.with(row);
            evaluatableExpression.evaluate();
            evaluated[index] = true;
        }
        return evaluatableExpression.resultValue();
    }

    @Override
    public HKey hKey()
    {
        return null;
    }

    // ProjectedRow interface

    public ProjectedRow(ProjectedRowType rowType,
                        Row row,
                        QueryContext context,
                        QueryBindings bindings,
                        List<TEvaluatableExpression> pEvaluatableExprs)
    {
        this.context = context;
        this.bindings = bindings;
        this.rowType = rowType;
        this.row = row;
        this.pEvaluatableExpressions = pEvaluatableExprs;
        if (pEvaluatableExpressions == null)
            evaluated = null;
        else
            evaluated = new boolean[pEvaluatableExpressions.size()];
    }

    public static List<TEvaluatableExpression> createTEvaluatableExpressions
        (List<? extends TPreparedExpression> pExpressions)
    {
        if (pExpressions == null)
            return null;
        int n = pExpressions.size();
        List<TEvaluatableExpression> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            TEvaluatableExpression eval = pExpressions.get(i).build();
            result.add(eval);
        }
        return result;
    }


    // Object state

    private final QueryContext context;
    private final QueryBindings bindings;
    private final ProjectedRowType rowType;
    private final Row row;
    private final List<TEvaluatableExpression> pEvaluatableExpressions;
    private final boolean[] evaluated;
}
