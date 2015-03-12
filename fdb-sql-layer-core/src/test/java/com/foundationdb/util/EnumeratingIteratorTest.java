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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public final class EnumeratingIteratorTest
{
    @Test
    public void testOf()
    {
        List<Character> list = Arrays.asList('a', 'b', 'c', 'd');

        int localCount = 0;
        for (Enumerated<Character> countingChar : EnumeratingIterator.of(list))
        {
            int counted = countingChar.count();
            assertEquals("count", localCount, counted);
            assertEquals("value", list.get(localCount), countingChar.get());
            ++localCount;
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void cannotRemove()
    {
        List<Character> list = Arrays.asList('e', 'f', 'g', 'h');
        Iterator<Enumerated<Character>> iterator = EnumeratingIterator.of(list).iterator();

        assertTrue("has next", iterator.hasNext());
        assertEquals("value", Character.valueOf('e'), iterator.next().get());
        iterator.remove();
    }
}
