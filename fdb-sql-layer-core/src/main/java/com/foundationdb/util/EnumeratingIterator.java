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

public final class EnumeratingIterator<T> implements Iterable<Enumerated<T>>
{
    private static class InternalEnumerated<T> implements Enumerated<T>
    {
        private final T elem;
        private final int count;

        private InternalEnumerated(T elem, int count)
        {
            this.elem = elem;
            this.count = count;
        }

        @Override
        public T get()
        {
            return elem;
        }

        @Override
        public int count()
        {
            return count;
        }
    }

    private final class InternalIterator<T> implements Iterator<Enumerated<T>>
    {
        private int counter = 0;
        private final Iterator<T> iter;

        private InternalIterator(Iterator<T> iter)
        {
            this.iter = iter;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public Enumerated<T> next()
        {
            return new InternalEnumerated<>(iter.next(), counter++);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("removal within an enumerating iterator is not defined");
        }
    }

    private final Iterable<T> iterable;

    public static <T> EnumeratingIterator<T> of(Iterable<T> iterable)
    {
        return new EnumeratingIterator<>(iterable);
    }

    public EnumeratingIterator(Iterable<T> iterable)
    {
        this.iterable = iterable;
    }

    @Override
    public Iterator<Enumerated<T>> iterator()
    {
        return new InternalIterator<>(iterable.iterator());
    }
}
