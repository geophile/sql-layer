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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public final class FilteringIteratorTest {

    @Test
    public void correctUsageBasic() {
        ArrayList<Integer> list = list(1, 2, 3, 4, 5, 6);
        List<Integer> filtered = dump( onlyEvens(list) );
        assertEquals("filtered list", list(2, 4, 6), filtered);
    }

    // have to filter out more than one in a row
    @Test
    public void correctUsageBasicSparse() {
        ArrayList<Integer> list = list(1, 27, 31, 11, 2, 3, 4, 5, 6);
        List<Integer> filtered = dump( onlyEvens(list) );
        assertEquals("filtered list", list(2, 4, 6), filtered);
    }

    @Test
    public void correctUsageEmptyList() {
        List<Integer> filtered = dump( onlyEvens() );
        assertEquals("filtered list", list(), filtered);
    }

    @Test
    public void correctUsageEmptyAfterFilter() {
        List<Integer> filtered = dump( onlyEvens(1, 3, 7) );
        assertEquals("filtered list", list(), filtered);
    }

    @Test
    public void correctUsageRemove() {
        ArrayList<Integer> list = list(1, 2, 3, 4, 5, 6);
        Iterator<Integer> iterator = onlyEvens(list, true);
        List<Integer> removed = new ArrayList<>();
        while (iterator.hasNext()) {
            removed.add( iterator.next() );
            iterator.remove();
        }
        assertEquals("removed", list(2, 4, 6), removed);
        assertEquals("left", list(1, 3, 5), list);
    }

    @Test(expected=NoSuchElementException.class)
    public void nextOnEmpty() {
        Iterator<Integer> iterator = onlyEvens();
        iterator.next();
    }

    @Test(expected=NoSuchElementException.class)
    public void nextOnEmptyAfterFilter() {
        Iterator<Integer> iterator = onlyEvens(1);
        iterator.next();
    }

    @Test
    public void nextTwice() {
        Iterator<Integer> iterator = onlyEvens(1, 2, 3, 4, 5);
        assertEquals("first", 2, iterator.next().intValue());
        assertEquals("second", 4, iterator.next().intValue());
        assertFalse("hasNext() == true", iterator.hasNext());
    }

    @Test(expected=IllegalStateException.class)
    public void removeTwice() {
        Iterator<Integer> iterator = SafeAction.of(new SafeAction.Get<Iterator<Integer>>() {
            @Override
            public Iterator<Integer> get() {
                Iterator<Integer> iterator = onlyEvens(1, 2);
                assertTrue("hasNext == false", iterator.hasNext());
                assertEquals("next()", 2, iterator.next().intValue());
                iterator.remove();
                return iterator;
            }
        });
        iterator.remove();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void removeDisallowedByIterator() {
        Iterator<Integer> iterator = SafeAction.of(new SafeAction.Get<Iterator<Integer>>() {
            @Override
            public Iterator<Integer> get() {
                Iterator<Integer> iterator = onlyEvens(list(2), false);
                assertTrue("hasNext == false", iterator.hasNext());
                assertEquals("next()", 2, iterator.next().intValue());
                return iterator;
            }
        });
        iterator.remove();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void removeDisallowedByDelegate() {
        Iterator<Integer> iterator = SafeAction.of(new SafeAction.Get<Iterator<Integer>>() {
            @Override
            public Iterator<Integer> get() {
                Iterator<Integer> iterator = onlyEvens(Collections.unmodifiableList(list(2)), false);
                assertTrue("hasNext == false", iterator.hasNext());
                assertEquals("next()", 2, iterator.next().intValue());
                return iterator;
            }
        });
        iterator.remove();
    }

    @Test
    public void hasNextTwice() {
        Iterator<Integer> iterator = onlyEvens(1, 2, 3);
        assertTrue("hasNext 1", iterator.hasNext());
        assertTrue("hasNext 2", iterator.hasNext());
        assertEquals("next", 2, iterator.next().intValue());
        assertFalse("hasNext 3", iterator.hasNext());
    }

    private static Iterator<Integer> onlyEvens(int... integers) {
        return onlyEvens(list(integers));
    }

    private static Iterator<Integer> onlyEvens(Iterable<Integer> iterable) {
        return onlyEvens(iterable, true);
    }

    private static Iterator<Integer> onlyEvens(Iterable<Integer> iterable, boolean mutable) {
        return new FilteringIterator<Integer>(iterable.iterator(), mutable) {
            @Override
            protected boolean allow(Integer item) {
                return (item % 2) == 0;
            }
        };
    }

    private static List<Integer> dump(Iterator<Integer> iterator) {
        ArrayList<Integer> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    private static ArrayList<Integer> list(int... values) {
        ArrayList<Integer> list = new ArrayList<>(values.length);
        for (int value : values) {
            list.add(value);
        }
        return list;
    }
}
