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
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.LazyListBase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.SparseArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class TPreparedFunction implements TPreparedExpression {

    @Override
    public TInstance resultType() {
        return resultType;
    }

    @Override
    public TPreptimeValue evaluateConstant(final QueryContext queryContext) {
        LazyList<TPreptimeValue> lazyInputs = new LazyListBase<TPreptimeValue>() {
            @Override
            public TPreptimeValue get(int i) {
                return inputs.get(i).evaluateConstant(queryContext);
            }

            @Override
            public int size() {
                return inputs.size();
            }
        };
        TPreptimeContext preptimeContext = new TPreptimeContext(inputTypes, resultType, queryContext, preptimeValues);
        return overload.evaluateConstant(preptimeContext, lazyInputs);
    }

    @Override
    public TEvaluatableExpression build() {
        List<TEvaluatableExpression> children = new ArrayList<>(inputs.size());
        for (TPreparedExpression input : inputs)
            children.add(input.build());
        TExecutionContext executionContext = 
            new TExecutionContext(preptimeValues,
                                  inputTypes,
                                  resultType,
                                  null,
                                  null, null, null);
        return new TEvaluatableFunction(
                overload,
                resultType,
                children,
                executionContext);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return overload.getExplainer(context, inputs, resultType);
    }

    @Override
    public String toString() {
        return overload.toString(inputs, resultType);
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    public TPreparedFunction(TValidatedScalar overload,
                             TInstance resultType,
                             List<? extends TPreparedExpression> inputs)
    {
        this(overload, resultType, inputs, null);
    }

    public TPreparedFunction(TValidatedScalar overload,
                             TInstance resultType,
                             List<? extends TPreparedExpression> inputs,
                             SparseArray<Object> preptimeValues)
    {
        this.overload = overload;
        this.resultType = resultType;
        this.inputTypes = new ArrayList<>(inputs.size());
        for (TPreparedExpression input : inputs) {
            inputTypes.add(input.resultType());
        }
        this.inputs = inputs;
        this.preptimeValues = preptimeValues;
    }

    private final TValidatedScalar overload;
    private final TInstance resultType;
    private final List<TInstance> inputTypes;
    private final List<? extends TPreparedExpression> inputs;
    private final SparseArray<Object> preptimeValues;

    private static final class TEvaluatableFunction implements TEvaluatableExpression {

        @Override
        public void with(Row row) {
            for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
                TEvaluatableExpression input = inputs.get(i);
                input.with(row);
                inputValues[i] = null;
            }
        }

        @Override
        public void with(QueryContext context) {
            this.context.setQueryContext(context);
            for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
                TEvaluatableExpression input = inputs.get(i);
                input.with(context);
                inputValues[i] = null;
            }
        }

        @Override
        public void with(QueryBindings bindings) {
            for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
                TEvaluatableExpression input = inputs.get(i);
                input.with(bindings);
                inputValues[i] = null;
            }
        }

        @Override
        public ValueSource resultValue() {
            return resultValue;
        }

        @Override
        public void evaluate() {
            Arrays.fill(inputValues, null);
            overload.evaluate(context, evaluations, resultValue);
        }

        public TEvaluatableFunction(TValidatedScalar overload,
                                    TInstance resultType,
                                    final List<? extends TEvaluatableExpression> inputs,
                                    TExecutionContext context)
        {
            this.overload = overload;
            this.inputs = inputs;
            this.inputValues = new ValueSource[inputs.size()];
            this.context = context;
            resultValue = new Value(resultType);
            this.evaluations = new LazyListBase<ValueSource>() {
                @Override
                public ValueSource get(int i) {
                    ValueSource value = inputValues[i];
                    if (value == null) {
                        TEvaluatableExpression inputExpr = inputs.get(i);
                        inputExpr.evaluate();
                        value = inputExpr.resultValue();
                        inputValues[i] = value;
                    }
                    return value;
                }

                @Override
                public int size() {
                    return inputValues.length;
                }
            };
        }

        private final TValidatedScalar overload;
        private final ValueSource[] inputValues;
        private final List<? extends TEvaluatableExpression> inputs;
        private final Value resultValue;
        private final TExecutionContext context;
        private final LazyList<? extends ValueSource> evaluations;
    }
}
