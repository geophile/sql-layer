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

import com.foundationdb.util.AssertUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class SplitHandlerTest {
    @Test
    public void singleStream() {
        List<String> inputs = Arrays.asList(
                "one",
                "one",
                "two",
                "three",
                "three",
                "three"
        );
        List<SplitPair> onlyStream = Arrays.asList(
                new SplitPair("one", 2),
                new SplitPair("two", 1),
                new SplitPair("three", 3)
        );

        List<List<SplitPair>> expected = Collections.singletonList(onlyStream);
        List<List<SplitPair>> actual = run(new BucketTestUtils.SingletonSplitter<String>(), inputs);
        AssertUtils.assertCollectionEquals("single stream", expected, actual);
    }

    @Test
    public void twoStreamsSameLength() {
        List<String> inputs = Arrays.asList(
                "one a",
                "one a",
                "two b",
                "three c",
                "three c",
                "three c"
        );
        List<SplitPair> firstStream = Arrays.asList(
                new SplitPair("one", 2),
                new SplitPair("two", 1),
                new SplitPair("three", 3)
        );
        List<SplitPair> secondStream = Arrays.asList(
                new SplitPair("a", 2),
                new SplitPair("b", 1),
                new SplitPair("c", 3)
        );
        
        Splitter<String> splitter = new Splitter<String>() {
            @Override
            public int segments() {
                return 2;
            }

            @Override
            public List<? extends String> split(String input) {
                return Arrays.asList(input.split(" "));
            }
        };

        List<List<SplitPair>> expected = new ArrayList<>();
        expected.add(firstStream);
        expected.add(secondStream);

        List<List<SplitPair>> actual = run(splitter, inputs);
        AssertUtils.assertCollectionEquals("single stream", expected, actual);
    }

    @Test
    public void twoStreamsDifferentLength() {
        List<String> inputs = Arrays.asList(
                "one a",
                "one a",
                "two a",
                "three c",
                "three c",
                "three c"
        );
        List<SplitPair> firstStream = Arrays.asList(
                new SplitPair("one", 2),
                new SplitPair("two", 1),
                new SplitPair("three", 3)
        );
        List<SplitPair> secondStream = Arrays.asList(
                new SplitPair("a", 3),
                new SplitPair("c", 3)
        );

        Splitter<String> splitter = new Splitter<String>() {
            @Override
            public int segments() {
                return 2;
            }

            @Override
            public List<? extends String> split(String input) {
                return Arrays.asList(input.split(" "));
            }
        };

        List<List<SplitPair>> expected = new ArrayList<>();
        expected.add(firstStream);
        expected.add(secondStream);


        List<List<SplitPair>> actual = run(splitter, inputs);
        AssertUtils.assertCollectionEquals("single stream", expected, actual);
    }
    
    @Test
    public void noInputs() {
        ToArraySplitHandler handler = new ToArraySplitHandler(new BucketTestUtils.SingletonSplitter<String>());
        handler.init();
        handler.finish();
        List<List<SplitPair>> actual = handler.splitPairStreams;
        List<SplitPair> emptyStream = Collections.emptyList();
        assertEquals("empty input results", Collections.singletonList(emptyStream), actual);
    }

    @Test(expected = IllegalStateException.class)
    public void visitBeforeInit() {
        ToArraySplitHandler handler = new ToArraySplitHandler(new BucketTestUtils.SingletonSplitter<String>());
        handler.visit("foo");
    }

    @Test(expected = IllegalStateException.class)
    public void finishBeforeInit() {
        ToArraySplitHandler handler = new ToArraySplitHandler(new BucketTestUtils.SingletonSplitter<String>());
        handler.finish();
    }

    @Test(expected = IllegalStateException.class)
    public void mismatchedSegmentsCount() {
        Splitter<String> splitter = new Splitter<String>() {
            @Override
            public int segments() {
                return 1; // lie! we'll be returning a list of size 2
            }

            @Override
            public List<? extends String> split(String input) {
                return Arrays.asList(input.split(" "));
            }
        };
        ToArraySplitHandler handler = new ToArraySplitHandler(splitter);
        handler.init();
        handler.visit("one two");
    }

    private List<List<SplitPair>> run(Splitter<String> splitter, List<String> inputs) {
        ToArraySplitHandler handler = new ToArraySplitHandler(splitter);
        handler.init();
        for (String input : inputs) {
            handler.visit(input);
        }
        handler.finish();

        return handler.splitPairStreams;
    }
    
    private static class ToArraySplitHandler extends SplitHandler<String> {
        @Override
        protected void handle(int segmentIndex, String input, int count) {
            SplitPair splitPair = new SplitPair(input, count);
            List<SplitPair> stream = splitPairStreams.get(segmentIndex);
            stream.add(splitPair);
        }

        private ToArraySplitHandler(Splitter<String> objectSplitter) {
            super(objectSplitter);
            int segments = objectSplitter.segments();
            splitPairStreams = new ArrayList<>(segments);
            for (int i = 0; i < segments; ++i) {
                splitPairStreams.add(new ArrayList<SplitPair>());
            }
        }

        final List<List<SplitPair>> splitPairStreams;
    }
    
    private static class SplitPair {
        
        private SplitPair(Object value, int count) {
            this.value = value;
            this.count = count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SplitPair splitPair = (SplitPair) o;
            return count == splitPair.count && value.equals(splitPair.value);

        }

        @Override
        public int hashCode() {
            int result = value.hashCode();
            result = 31 * result + count;
            return result;
        }

        @Override
        public String toString() {
            return String.format("(%s)x%d", value, count);
        }

        final Object value;
        final int count;
    }
}
