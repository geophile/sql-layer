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
package com.foundationdb.qp.operator;

/**
 * A single {@link QueryBindings} stream.
 */
public class SingletonQueryBindingsCursor implements QueryBindingsCursor
{
    enum State { CLOSED, PENDING, EXHAUSTED };
    private QueryBindings bindings;
    private State state;

    public SingletonQueryBindingsCursor(QueryBindings bindings) {
        reset(bindings);
    }

    @Override
    public void openBindings() {
        state = State.PENDING;
    }

    @Override
    public QueryBindings nextBindings() {
        switch (state) {
        case CLOSED:
            throw new IllegalStateException("Bindings cursor not open");
        case PENDING:
            state = State.EXHAUSTED;
            return bindings;
        case EXHAUSTED:
        default:
            return null;
        }
    }

    @Override
    public void closeBindings() {
        state = State.CLOSED;
    }

    @Override
    public void cancelBindings(QueryBindings ancestor) {
        if ((state == State.PENDING) && bindings.isAncestor(ancestor)) {
            state = State.EXHAUSTED;
        }
    }

    public void reset(QueryBindings bindings) {
        this.bindings = bindings;
        this.state = State.CLOSED;
    }
}
