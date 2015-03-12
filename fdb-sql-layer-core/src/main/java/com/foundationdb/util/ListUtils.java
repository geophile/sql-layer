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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public final class ListUtils {

    /**
     * Ensures that there are no more than {@code size} elements in the given {@code list}. If the list already has
     * {@code size} elements or fewer, this doesn't do anything; otherwise, it'll remove enough elements from the
     * list to ensure that {@code list.size() == size}. Either way, by the end of this invocation,
     * {@code list.size() <= size}.
     * @param list the incoming list.
     * @param size the maximum number of elements to keep in {@code list}
     * @throws IllegalArgumentException if {@code size < 0}
     * @throws NullPointerException if {@code list} is {@code null}
     * @throws UnsupportedOperationException if the list doesn't support removal
     */
    public static void truncate(List<?> list, int size) {
        ArgumentValidation.isGTE("truncate size", size, 0);

        int rowsToRemove = list.size() - size;
        if (rowsToRemove <= 0) {
            return;
        }
        ListIterator<?> iterator = list.listIterator(list.size());
        while (rowsToRemove-- > 0) {
            iterator.previous();
            iterator.remove();
        }
    }

    public static void removeDuplicates(List<?> list) {
        Set<Object> elems = new HashSet<>(list.size());
        for(Iterator<?> iter = list.iterator(); iter.hasNext();) {
            Object next = iter.next();
            if (!elems.add(next))
                iter.remove();
        }
    }
}
