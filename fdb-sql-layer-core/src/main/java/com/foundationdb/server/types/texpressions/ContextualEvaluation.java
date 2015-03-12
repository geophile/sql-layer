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
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

abstract class ContextualEvaluation<T> implements TEvaluatableExpression {

    @Override
    public void with(Row row) {
    }

    @Override
    public void with(QueryContext context) {
    }

    @Override
    public void with(QueryBindings bindings) {
    }

    @Override
    public ValueSource resultValue() {
        if (readyValue == null)
            throw new IllegalStateException("haven't evaluated since having seen a new context");
        return readyValue;
    }

    @Override
    public void evaluate() {
        if (context == null)
            throw new IllegalStateException("no context given");
        if (readyValue == null) {
            if (unreadyValue == null) {
                // first evaluation of this expression
                readyValue = new Value(underlyingType);
            }
            else {
                // readyValue is null, unreadyValue is not null. Means we've seen a QueryContext but have
                // not evaluated it. Set the readyValue to unreadyValue, as we've about to evaluate it.
                readyValue = unreadyValue;
                unreadyValue = null;
            }
        }
        evaluate(context, readyValue);
    }

    protected void setContext(T context) {
        if (unreadyValue == null) {
            // give unreadValue the readyValue, whatever it is (it could be null if we've never evaluated)
            // then set readyValue to null
            unreadyValue = readyValue;
            readyValue = null;
        }
        this.context = context;
    }
    
    protected abstract void evaluate(T context, ValueTarget target);

    protected ContextualEvaluation(TInstance underlyingType) {
        this.underlyingType = underlyingType;
    }

    // At most one of these will be non-null. If no QueryContext has been seen yet, they'll both be null.
    // Otherwise, unreadyValue means we've seen but not evaluated a QueryContext, and readyValue means we've
    // seen a QueryContext and evaluated it.
    private Value unreadyValue;
    private Value readyValue;
    private T context;
    private TInstance underlyingType;
}
