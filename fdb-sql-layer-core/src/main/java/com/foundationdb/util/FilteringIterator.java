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
package com.foundationdb.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator which filters out another iterator. The filtering is done via an abstract method, which lets us
 * avoid unnecessary allocations (of a seperate predicate object). This method comes in two flavors, immutable
 * and mutable; for obvious reasons, mutability is only supported if the delegate iterator is also mutable.
 * @param <T> the type to iterate
 */
public abstract class FilteringIterator<T> implements Iterator<T> {

    private static enum State {
        /**
         * This iterator is freshly created. It hasn't asked its delegate anything.
         */
        FRESH,
        /**
         * An item has been requested of the delegate, and it's pending to be delivered to this iterator's user.
         */
        NEXT_PENDING,
        /**
         * An item has been requested of the delegate, but it's already been delivered to this iterator's user.
         */
        NEXT_RETRIEVED,
        /**
         * There are no items left to deliver.
         */
        DONE
    }

    private final Iterator<T> delegate;
    private final boolean isMutable;
    private T next;
    private State state;

    public FilteringIterator(Iterator<T> delegate, boolean isMutable) {
        this.delegate = delegate;
        this.isMutable = isMutable;
        this.state = State.FRESH;

    }

    protected abstract boolean allow(T item);

    @Override
    public boolean hasNext() {
        switch (state) {
            case NEXT_PENDING:
                return true;
            case FRESH:
            case NEXT_RETRIEVED:
                advance();
                assert (state == State.NEXT_PENDING) || (state == State.DONE) : state;
                return state == State.NEXT_PENDING;
            case DONE:
                return false;
            default:
                throw new AssertionError("FilteringIterator has unknown internal state: " + state);
        }
    }

    @Override
    public T next() {
        if (state == State.FRESH || state == State.NEXT_RETRIEVED) {
            advance();
        }
        if (state == State.DONE) {
            throw new NoSuchElementException();
        }
        assert state == State.NEXT_PENDING : state;
        state = State.NEXT_RETRIEVED;
        return next;
    }

    @Override
    public void remove() {
        if (!isMutable) {
            throw new UnsupportedOperationException();
        }
        if (state != State.NEXT_RETRIEVED) {
            throw new IllegalStateException(state.name());
        }
        delegate.remove();
    }

    /**
     * Advances the delegate until we get an element which passes the filter,
     *
     * Coming into this method, this iterator's state must be FRESH or NEXT_RETRIEVED. When this method completes
     * successfully, the state will be DONE or NEXT_PENDING.
     *
     * At the end of this method, this FilteringIterator's state will be either DONE or NEXT_KNOWN. This method
     * should only be invoked when the state is NEEDS_NEXT. If the state of this iterator is NEXT_KNOWN on return,
     * the "next" field will point to the correct item.
     */
    private void advance() {
        assert (state == State.FRESH) || (state == State.NEXT_RETRIEVED) : state;
        while (state != State.NEXT_PENDING) {
            if (!delegate.hasNext()) {
                state = State.DONE;
                return;
            } else {
                T item = delegate.next();
                if (allow(item)) {
                    next = item;
                    state = State.NEXT_PENDING;
                }
            }
        }
    }
}
