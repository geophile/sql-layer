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

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

import java.util.List;

public final class RowBasedUnboundExpressions implements UnboundExpressions {
    @Override
    public ValueRecord get(QueryContext context, QueryBindings bindings) {
        return new ExpressionsAndBindings(rowType, pExprs, context, bindings);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        for (TPreparedExpression expression : pExprs) {
            atts.put(Label.EXPRESSIONS, expression.getExplainer(context));
        }
        return new CompoundExplainer(Type.ROW, atts);
    }

    @Override
    public String toString() {
        return "UnboundExpressions" + pExprs;
    }

    // The extra, unused boolean parameter is to allow java to cleanly 
    // distinguish between this method and the next one. 
    public RowBasedUnboundExpressions(RowType rowType, List<ExpressionGenerator> expressions, boolean generate) {
        this(rowType, API.generateNew(expressions));
    }

    public RowBasedUnboundExpressions(RowType rowType, List<TPreparedExpression> pExprs)
    {
        assert pExprs != null : "must supply pExprs";
        for (TPreparedExpression expression : pExprs) {
            if (expression == null) {
                throw new IllegalArgumentException();
            }
        }
        this.pExprs = pExprs;
        this.rowType = rowType.schema().newProjectType(pExprs);
    }

    @Override
    public boolean isLiteral(int index) {
        return pExprs.get(index).isLiteral();
    }

    private final List<TPreparedExpression> pExprs;
    private final RowType rowType;
    private static class ExpressionsAndBindings implements ValueRecord {

        @Override
        public ValueSource value(int index) {
            return expressionRow.value(index);
        }

        ExpressionsAndBindings(RowType rowType, List<TPreparedExpression> pExprs,
                               QueryContext context, QueryBindings bindings)
        {
            expressionRow = new ExpressionRow(rowType, context, bindings, pExprs);
        }
        
        @Override
        public String toString() {
            return expressionRow.toString();
        }

        private final ExpressionRow expressionRow;
        
        
    }
}
