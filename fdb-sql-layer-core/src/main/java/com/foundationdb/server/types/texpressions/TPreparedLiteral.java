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
import com.foundationdb.util.AkibanAppender;

public final class TPreparedLiteral implements TPreparedExpression {
    
    @Override
    public TInstance resultType() {
        return type;
    }

    @Override
    public TEvaluatableExpression build() {
        return new Evaluation(value);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.LITERAL, "Literal", context);
        StringBuilder sql = new StringBuilder();
        if (type == null)
            sql.append("NULL");
        else
            type.formatAsLiteral(value, AkibanAppender.of(sql));
        ex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance(sql.toString()));
        return ex;
    }

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        if (type == null)
            return new TPreptimeValue(null, null);
        else
            return new TPreptimeValue(type, value);
    }

    @Override
    public String toString() {
        if (type == null)
            return "LiteralNull";
        StringBuilder sb = new StringBuilder("Literal(");
        type.format(value, AkibanAppender.of(sb));
        sb.append(')');
        return sb.toString();
    }

    @Override
    public boolean isLiteral() {
        return true;
    }

    public TPreparedLiteral(TInstance type, ValueSource value) {
        if (type == null) {
            this.type = null;
            this.value = ValueSources.getNullSource(null);
        }
        else {
            this.type = type;
            this.value = value;
        }
    }
    
    public TPreparedLiteral (ValueSource value) {
        this.type = value.getType();
        this.value = value;
    }

    private final TInstance type;
    private final ValueSource value;

    private static class Evaluation implements TEvaluatableExpression {
        @Override
        public ValueSource resultValue() {
            return value;
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

        private Evaluation(ValueSource value) {
            this.value = value;
        }

        private final ValueSource value;
    }
}
