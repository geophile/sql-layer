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

import com.foundationdb.ais.model.Sequence;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

public class TSequenceNextValueExpression implements TPreparedExpression
{
    private final TInstance resultType;
    private final Sequence sequence;

    public TSequenceNextValueExpression(TInstance resultType, Sequence sequence) {
        assert resultType != null;
        assert sequence != null;
        this.resultType = resultType;
        this.sequence = sequence;
    }

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        // Never constant
        return null;
    }

    @Override
    public TInstance resultType() {
        return resultType;
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes states = new Attributes();
        states.put(Label.NAME, PrimitiveExplainer.getInstance("_SEQ_NEXT"));
        states.put(Label.OPERAND, PrimitiveExplainer.getInstance(sequence.getSequenceName().getSchemaName()));
        states.put(Label.OPERAND, PrimitiveExplainer.getInstance(sequence.getSequenceName().getTableName()));
        return new CompoundExplainer(Type.FUNCTION, states);
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    private class InnerEvaluation implements TEvaluatableExpression
    {
        private QueryContext context;
        private Value value;

        @Override
        public ValueSource resultValue() {
            return value;
        }

        @Override
        public void evaluate() {
            long next = context.getStore().sequenceNextValue(sequence);
            value = new Value(resultType);
            ValueSources.valueFromLong(next, value);
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
            this.context = context;
            this.value = null;
        }

        @Override
        public void with(QueryBindings bindings) {
        }
    }
}
