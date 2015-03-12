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
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class BloomFilterTest
{
    @Test
    public void test()
    {
        for (double errorRate : ERROR_RATES) {
            for (int count : COUNTS) {
                test("dense longs", errorRate, count, denseLongs(0, count), denseLongs(count, 2 * count));
                test("sparse longs", errorRate, count, sparseLongs(0, count), sparseLongs(count, 2 * count));
                test("patterned strings", errorRate, count, patternedStrings(0, count), patternedStrings(count, 2 * count));
                test("random strings", errorRate, count, randomStrings(0, count), randomStrings(count, 2 * count));
            }
        }
    }

    private void test(String label, double errorRate, int count, List keys, List missingKeys)
    {
        BloomFilter filter = new BloomFilter(count, errorRate);
        for (Object key : keys) {
            filter.add(key.hashCode());
        }
        // Check that all keys are found
        for (Object key : keys) {
            assertTrue(filter.maybePresent(key.hashCode()));
        }
        // Check false positives for missing keys
        int falsePositives = 0;
        for (Object missingKey : missingKeys) {
            if (filter.maybePresent(missingKey.hashCode())) {
                falsePositives++;
            }
        }
        double actualErrorRate = ((double) falsePositives) / count;
        double maxAcceptableErrorRate = errorRate * 10;
        assertTrue(actualErrorRate <= maxAcceptableErrorRate);
    }

    @SuppressWarnings("unchecked")
    private List denseLongs(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            keys.add(i);
        }
        return keys;
    }

    @SuppressWarnings("unchecked")
    private List sparseLongs(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            long key = (((long) random.nextInt()) << 32) | i;
            keys.add(key);
        }
        return keys;
    }

    @SuppressWarnings("unchecked")
    private List patternedStrings(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            String key = String.format("img%09d.jpg", i);
            keys.add(key);
        }
        return keys;
    }

    @SuppressWarnings("unchecked")
    private List randomStrings(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            long key = (((long) random.nextInt()) << 32) | i;
            keys.add(Long.toString(key));
        }
        return keys;
    }

    private static final Random random = new Random(419);
    private static final double[] ERROR_RATES = { 0.10d, 0.1d, 0.01d, 0.001d, 0.0001d };
    private static final int[] COUNTS = { 100, 1000, 10000, 100000 };
}
