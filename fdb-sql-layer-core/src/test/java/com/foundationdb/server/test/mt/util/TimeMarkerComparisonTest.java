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
package com.foundationdb.server.test.mt.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class TimeMarkerComparisonTest
{
    @Test
    public void noTimestampConflicts() {
        ListMultimap<Long,String> alpha = newMultimap();
        alpha.put(1L, "one");
        alpha.put(3L, "three");
        ListMultimap<Long,String> beta = newMultimap();
        beta.put(2L, "two");
        test("[[one], [two], [three]]", alpha, beta);
    }

    @Test
    public void conflictsWithinOne() {
        ListMultimap<Long,String> alpha = newMultimap();
        alpha.put(1L, "oneA");
        alpha.put(1L, "oneB");
        ListMultimap<Long,String> beta = newMultimap();
        beta.put(2L, "two");
        test("[[oneA], [oneB], [two]]", alpha, beta);
    }

    @Test
    public void conflictsAcrossTwo() {
        ListMultimap<Long,String> alpha = newMultimap();
        alpha.put(1L, "one");
        alpha.put(2L, "twoA");
        ListMultimap<Long,String> beta = newMultimap();
        beta.put(2L, "twoB");
        test("[[one], [twoA, twoB]]", alpha, beta);
    }

    @Test
    public void multipleConflicts() {
        ListMultimap<Long,String> alpha = newMultimap();
        alpha.put(1L, "oneXA");
        alpha.put(1L, "oneYA");
        ListMultimap<Long,String> beta = newMultimap();
        beta.put(1L, "oneYB");
        beta.put(1L, "oneXB");
        test("[[oneXA, oneYB], [oneXB, oneYA]]", alpha, beta);
    }

    private static <K,V> ListMultimap<K,V> newMultimap() {
        return ArrayListMultimap.create();
    }

    private static List<List<String>> combine(ListMultimap<Long, String> a, ListMultimap<Long, String> b) {
        return TimeMarkerComparison.combineTimeMarkers(Arrays.asList(new TimeMarker(a), new TimeMarker(b)));
    }

    private static void test(String expected, ListMultimap<Long,String> a, ListMultimap<Long,String> b) {
        assertEquals("flattened list", expected, combine(a, b).toString());
    }
}
