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
package com.foundationdb.sql.server;

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.explain.std.TExpressionExplainer;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.value.ValueSource;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public abstract class ServerJavaRoutineTExpression implements TPreparedExpression {
    protected final Routine routine;
    private final List<? extends TPreparedExpression> inputs;

    protected ServerJavaRoutineTExpression(Routine routine,
                                           List<? extends TPreparedExpression> inputs) {
        this.routine = routine;
        this.inputs = inputs;
    }
     
    @Override
    public TPreptimeValue evaluateConstant(QueryContext context) {
        List<TPreptimeValue> values = new ArrayList<>(inputs.size());
        boolean allConstant = true, anyNull = false;
        ValueSource constantSource = null;
        for (TPreparedExpression input : inputs) {
            TPreptimeValue value = input.evaluateConstant(context);
            values.add(value);
            if (value.value() == null) {
                allConstant = false;
            }
            else if (value.value().isNull()) {
                anyNull = true;
            }
        }
        if (allConstant && routine.isDeterministic()) {
            ValueRoutineInvocation invocation = new TPreptimeValueRoutineInvocation(routine, values);
            ServerJavaRoutine javaRoutine = javaRoutine((ServerQueryContext)context, null, invocation);
            evaluate(javaRoutine);
            constantSource = invocation.getReturnValue();
        }
        if (anyNull && !routine.isCalledOnNullInput()) {
            constantSource = ValueSources.getNullSource(resultType());
        }
        return new TPreptimeValue(resultType(), constantSource);
    }

    @Override
    public TInstance resultType() {
        return routine.getReturnValue().getType();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer ex = new TExpressionExplainer(Type.FUNCTION, routine.getName().toString(), context);
        for (TPreparedExpression input : inputs) {
            ex.addAttribute(Label.OPERAND, input.getExplainer(context));
        }
        return ex;
    }

    @Override
    public TEvaluatableExpression build() {
        List<TEvaluatableExpression> evals = new ArrayList<>(inputs.size());
        for (TPreparedExpression input : inputs) {
            evals.add(input.build());
        }
        return new TEvaluatableJavaRoutine(routine, evals);
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    protected abstract ServerJavaRoutine javaRoutine(ServerQueryContext context,
                                                     QueryBindings bindings,
                                                     ServerRoutineInvocation invocation);

    protected void evaluate(ServerJavaRoutine javaRoutine) {
        javaRoutine.push();
        boolean success = false;
        try {
            javaRoutine.setInputs();
            javaRoutine.invoke();
            javaRoutine.getOutputs();
            success = true;
        }
        finally {
            javaRoutine.pop(success);
        }
    }

    static abstract class ValueRoutineInvocation extends ServerRoutineInvocation {
        protected Value returnValue;

        protected ValueRoutineInvocation(Routine routine) {
            super(routine);
            returnValue = new Value(routine.getReturnValue().getType());
        }

        public ValueSource getReturnValue() {
            return returnValue;
        }
    }

    static abstract class ValueInvocationValues extends ServerJavaValues {
        private Routine routine;
        private ServerQueryContext context;
        private Value returnValue;
        
        protected ValueInvocationValues(Routine routine,
                                        ServerQueryContext context,
                                        Value returnValue) {
            this.routine = routine;
            this.context = context;
            this.returnValue = returnValue;
        }

        @Override
        protected ServerQueryContext getContext() {
            return context;
        }

        @Override
        protected TInstance getType(int index) {
            Parameter parameter;
            if (index == RETURN_VALUE_INDEX)
                parameter = routine.getReturnValue();
            else
                parameter = routine.getParameters().get(index);
            return parameter.getType();
        }

        @Override
        protected void setValue(int index, ValueSource source) {
            assert (index == RETURN_VALUE_INDEX);
            if (source == null)
                returnValue.putNull();
            else
                ValueTargets.copyFrom(source, returnValue);
        }

        @Override
        protected ResultSet toResultSet(int index, Object resultSet) {
            throw new UnsupportedOperationException();
        }
    }

    static class TPreptimeValueRoutineInvocation extends ValueRoutineInvocation {
        private List<TPreptimeValue> inputs;

        public TPreptimeValueRoutineInvocation(Routine routine,
                                               List<TPreptimeValue> inputs) {
            super(routine);
            this.inputs = inputs;
        }

        @Override
        public ServerJavaValues asValues(ServerQueryContext context, QueryBindings bindings) {
            return new ValueInvocationValues(getRoutine(), context, returnValue) {
                    @Override
                    protected int size() {
                        return inputs.size();
                    }

                    @Override
                    protected ValueSource getValue(int index) {
                        return inputs.get(index).value();
                    }
                };
        }
    }

    static class TEvaluatableValueRoutineInvocation extends ValueRoutineInvocation {
        private List<TEvaluatableExpression> inputs;

        public TEvaluatableValueRoutineInvocation(Routine routine,
                                                  List<TEvaluatableExpression> inputs) {
            super(routine);
            this.inputs = inputs;
        }

        @Override
        public ServerJavaValues asValues(ServerQueryContext context, QueryBindings bindings) {
            return new ValueInvocationValues(getRoutine(), context, returnValue) {
                    @Override
                    protected int size() {
                        return inputs.size();
                    }

                    @Override
                    protected ValueSource getValue(int index) {
                        return inputs.get(index).resultValue();
                    }
                };
        }
    }

    class TEvaluatableJavaRoutine implements TEvaluatableExpression {
        private Routine routine;
        private List<TEvaluatableExpression> inputs;
        private ServerQueryContext context;
        private ValueRoutineInvocation invocation;
        private ServerJavaRoutine javaRoutine;

        public TEvaluatableJavaRoutine(Routine routine,
                                       List<TEvaluatableExpression> inputs) {
            this.routine = routine;
            this.inputs = inputs;
        }

        @Override
        public void with(Row row) {
            for (TEvaluatableExpression input : inputs) {
                input.with(row);
            }
        }

        @Override
        public void with(QueryContext context) {
            for (TEvaluatableExpression input : inputs) {
                input.with(context);
            }
            this.context = (ServerQueryContext)context;
        }

        @Override
        public void with(QueryBindings bindings) {
            for (TEvaluatableExpression input : inputs) {
                input.with(bindings);
            }
            invocation = new TEvaluatableValueRoutineInvocation(routine, inputs);
            javaRoutine = javaRoutine(context, bindings, invocation);
        }

        @Override
        public ValueSource resultValue() {
            return invocation.getReturnValue();
        }

        @Override
        public void evaluate() {
            for (TEvaluatableExpression input : inputs) {
                input.evaluate();
            }
            ServerJavaRoutineTExpression.this.evaluate(javaRoutine);
        }
    }

}
