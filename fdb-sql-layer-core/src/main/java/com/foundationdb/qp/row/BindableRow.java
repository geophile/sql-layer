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

import com.foundationdb.qp.expression.ExpressionRow;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.ArgumentValidation;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public abstract class BindableRow {

    // BindableRow class interface

    public static BindableRow of(RowType rowType, List<? extends ExpressionGenerator> expressions) {
        return of(rowType, API.generateNew(expressions), null);
    }

    public static BindableRow of(RowType rowType,
                                 List<? extends TPreparedExpression> pExpressions,
                                 QueryContext queryContext) {
        Iterator<? extends ValueSource> newVals;
        ArgumentValidation.isEQ("rowType fields", rowType.nFields(), "expressions.size", pExpressions.size());
        for (TPreparedExpression expression : pExpressions) {
            TPreptimeValue tpv = expression.evaluateConstant(queryContext);
            if (tpv == null || tpv.value() == null)
                return new BindingExpressions(rowType, pExpressions);
        }
        newVals = new PExpressionEvaluator(pExpressions, queryContext);
        ImmutableRow holderRow = new ImmutableRow(rowType, newVals);
        return new Delegating(holderRow);
    }

    public static BindableRow of(Row row) {
        return new Delegating(strictCopy(row));
    }

    // BindableRow instance interface

    public abstract Row bind(QueryContext context, QueryBindings bindings);
    public abstract CompoundExplainer getExplainer(ExplainContext context);

    private static ImmutableRow strictCopy(Row input) {
        RowPCopier newCopier;
        newCopier = new RowPCopier(input);
        return new ImmutableRow(input.rowType(), newCopier);
    }

    // nested classes

    private static class BindingExpressions extends BindableRow {
        @Override
        public Row bind(QueryContext context, QueryBindings bindings) {
            return new ExpressionRow(rowType, context, bindings, pExprs);
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context) {
            Attributes atts = new Attributes();
            for (TPreparedExpression pexpr : pExprs) {
                atts.put(Label.EXPRESSIONS, pexpr.getExplainer(context));
            }
            return new CompoundExplainer(Type.ROW, atts);
        }

        private BindingExpressions(RowType rowType, List<? extends TPreparedExpression> pExprs)
        {
            this.rowType = rowType;
            this.pExprs = pExprs;
            /*
            if (expressions != null) {
                // TODO do we need an equivalent for pexprs?
                for (Expression expression : expressions) {
                    if (expression.needsRow()) {
                        throw new IllegalArgumentException("expression " + expression + " needs a row");
                    }
                }
            }
            */
        }

        // object interface


        @Override
        public String toString() {
            return "Bindable " + pExprs;
        }

        private final List<? extends TPreparedExpression> pExprs;
        private final RowType rowType;
    }

    private static class RowPCopier implements Iterator<ValueSource>  {

        @Override
        public boolean hasNext() {
            return i < sourceRow.rowType().nFields();
        }

        @Override
        public ValueSource next() {
            return sourceRow.value(i++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public RowPCopier(Row sourceRow) {
            this.sourceRow = sourceRow;
        }

        private final Row sourceRow;
        private int i = 0;
    }

    private static class PExpressionEvaluator implements Iterator<ValueSource> {

        @Override
        public boolean hasNext() {
            return expressions.hasNext();
        }

        @Override
        public ValueSource next() {
            TPreparedExpression expression = expressions.next();
            TPreptimeValue ptv = expression.evaluateConstant(context);
            assert ptv != null && ptv.value() != null
                    : "not constant: " + expression + " with prepare-time value of " + ptv;
            return ptv.value();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private PExpressionEvaluator(Collection<? extends TPreparedExpression> expressions, QueryContext context)
        {
            this.expressions = expressions.iterator();
            this.context = context;
        }

        private final Iterator<? extends TPreparedExpression> expressions;
        private final QueryContext context;
    }

    private static class Delegating extends BindableRow {
        @Override
        public Row bind(QueryContext context, QueryBindings bindings) {
            return row;
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context) {
            Attributes atts = new Attributes();
            for (int i = 0; i < row.rowType().nFields(); i++) {
                atts.put(Label.EXPRESSIONS, PrimitiveExplainer.getInstance(formatAsLiteral(i)));
            }
            return new CompoundExplainer(Type.ROW, atts);
        }

        private String formatAsLiteral(int i) {
            StringBuilder str = new StringBuilder();
            row.rowType().typeAt(i).formatAsLiteral(row.value(i), AkibanAppender.of(str));
            return str.toString();
        }

        @Override
        public String toString() {
            return String.valueOf(row);
        }

        private Delegating(ImmutableRow row) {
            this.row = row;
        }

        private final ImmutableRow row;
    }
}
