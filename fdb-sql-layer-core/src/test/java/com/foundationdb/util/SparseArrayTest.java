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

import java.util.List;

import static org.junit.Assert.*;

public final class SparseArrayTest {

    @Test
    public void getCreates() {
        CountingSparseArray tester = new CountingSparseArray();

        assertEquals("isDefined", false, tester.isDefined(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("isDefined", true, tester.isDefined(0));

        assertEquals("isDefined", false, tester.isDefined(5));
        assertEquals("second get", "2", tester.get(5));
        assertEquals("isDefined", true, tester.isDefined(5));

        assertEquals("create count", 2, tester.count);
    }
    
    @Test
    public void getGrowsCapacity() {
        CountingSparseArray tester = new CountingSparseArray();
        int oldCapacity = tester.currentCapacity();

        int index = oldCapacity + 10;
        assertEquals("isDefined", false, tester.isDefined(index));
        assertEquals("first get", "1", tester.get(index));
        assertEquals("isDefined", true, tester.isDefined(index));

        int newCapacity = tester.currentCapacity();
        assertTrue("capacity <= index: " + newCapacity+ " <= " + index, newCapacity > index);

        assertEquals("create count", 1, tester.count);
    }

    @Test
    public void getReuses() {
        CountingSparseArray tester = new CountingSparseArray();

        assertEquals("isDefined", false, tester.isDefined(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("isDefined", true, tester.isDefined(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("isDefined", true, tester.isDefined(0));
        assertEquals("create count", 1, tester.count);
    }
    
    @Test
    public void setIndex() {
        CountingSparseArray tester = new CountingSparseArray();

        assertEquals("isDefined", false, tester.isDefined(0));
        tester.set(0, "foo");
        assertEquals("create count", 0, tester.count);
        assertEquals("get", "foo", tester.get(0));
        assertEquals("create count", 0, tester.count);
        assertEquals("isDefined", true, tester.isDefined(0));
        
        String old = tester.set(0, "bar");
        assertEquals("get", "bar", tester.get(0));
        assertEquals("get", "foo", old);
        assertEquals("isDefined", true, tester.isDefined(0));
        assertEquals("create count", 0, tester.count);
    }
    
    @Test
    public void setGrowsCapacity() {
        CountingSparseArray tester = new CountingSparseArray();
        int oldCapacity = tester.currentCapacity();

        int index = oldCapacity + 10;
        assertEquals("isDefined", false, tester.isDefined(index));
        String old = tester.set(index, "foo");
        assertEquals("old value", null, old);
        assertEquals("isDefined", true, tester.isDefined(index));

        int newCapacity = tester.currentCapacity();
        assertTrue("capacity <= index: " + newCapacity+ " <= " + index, newCapacity > index);

        assertEquals("create count", 0, tester.count);
    }

    @Test
    public void toListIsCompact() {
        SparseArray<Integer> array = new SparseArray<>();
        array.set(0, 10);
        array.set(1, 11);
        List<Integer> list = array.toList();
        // Internal size may be bigger, but only get defined elements
        assertEquals("list size", 2, list.size());
        assertEquals("list get(0)", Integer.valueOf(10), list.get(0));
        assertEquals("list get(1)", Integer.valueOf(11), list.get(1));
    }

    @Test(expected=IllegalArgumentException.class)
    public void toListMustBeCompact() {
        SparseArray<Integer> array = new SparseArray<>();
        array.set(5, 50);
        array.toList();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void negativeIndex() {
        new CountingSparseArray().get(-1);
    }

    private static class CountingSparseArray extends SparseArray<String> {
        @Override
        protected String initialValue() {
            return Integer.toString(++count);
        }

        int count = 0;
    }
}
