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
package com.foundationdb.server.store.statistics.histograms;

import java.util.Arrays;
import java.util.List;

final class BucketTestUtils {
    public static <T> Bucket<T> bucket(T value, long equals, long lessThans, long lessThanDistincts) {
        Bucket<T> result = new Bucket<>();
        result.init(value, 1);
        for (int i = 1; i < equals; ++i) { // starting count from 1 since a new Bucket has equals of 1
            result.addEquals();
        }
        result.addLessThans(lessThans);
        result.addLessThanDistincts(lessThanDistincts);
        return result;
    }

    public static class SingletonSplitter<T> implements Splitter<T> {
        @Override
        public int segments() {
            return 1;
        }

        @Override
        public List<? extends T> split(T input) {
            oneElementList.set(0, input);
            return oneElementList;
        }

        @SuppressWarnings("unchecked")
        private List<T> oneElementList = Arrays.asList((T[])new Object[1]);
    }
}
