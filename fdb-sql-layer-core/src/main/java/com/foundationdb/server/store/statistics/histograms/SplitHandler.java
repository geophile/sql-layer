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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

abstract class SplitHandler<T> implements SampleVisitor<T> {

    protected abstract void handle(int segmentIndex, T input, int count);

    @Override
    public void init() {
        buffers = new ArrayList<>(segments);
        for (int i = 0; i < segments; ++i) {
            buffers.add(new SegmentBuffer<T>());
        }
    }

    @Override
    public void finish() {
        checkInit();
        for (int i = 0; i < segments; ++i) {
            SegmentBuffer<T> buffer = buffers.get(i);
            T segment = buffer.last();
            int count = buffer.lastCount();
            if (count > 0)
                handle(i, segment, count);
        }
    }

    @Override
    public List<? extends T> visit(T input) {
        checkInit();
        List<? extends T> split = splitter.split(input);
        if (split.size() != segments)
            throw new IllegalStateException("required " + segments + ", found " + split.size() + ": " + split);
        recycleBin.clear();
        for (int i = 0; i < segments; ++i) {
            T segment = split.get(i);
            SegmentBuffer<T> buffer = buffers.get(i);
            T prev = buffer.last();
            int count = buffer.put(segment, recycleBin);
            if (count > 0) {
                handle(i, prev, count);
            }
        }
        return recycleBin;
    }

    public SplitHandler(Splitter<T> splitter) {
        this.splitter = splitter;
        this.segments = splitter.segments();
        if (segments < 1)
            throw new IllegalArgumentException("splitter must provide at least 1 segment: " + segments);
        this.recycleBin = new ArrayList<>(segments);
    }

    private void checkInit() {
        if (buffers == null)
            throw new IllegalStateException("not initialized");
    }

    private final Splitter<T> splitter;
    private final int segments;
    private List<SegmentBuffer<T>> buffers;
    private final List<T> recycleBin;

    private static class SegmentBuffer<T> {
        /**
         * Adds an element to the stream. If that element is the same as the last element this buffer saw,
         * it won't be added to the stream, but will instead be recycled. If this element is different than the
         * one before, this class will return the number of times that other element had been seen. The caller
         * is responsible for having retrieved that element before calling this method.
         * @param segment the segment to add to the buffer
         * @param recycleBin where to put elements that need to be recycled
         * @return the number of times the last element appeared, or 0 if that's not known yet
         */
        int put(T segment, List<? super T> recycleBin) {
            int count;
            if (lastCount == 0) {
                // first element
                count = 0;
                lastCount = 1;
                last = segment;
            }
            else if (Objects.deepEquals(last, segment)) {
                // same segment, just update lastCount
                ++lastCount;
                count = 0;
                recycleBin.add(segment);
            } else {
                // new segment. Return and reset lastCount, and reset last
                count = lastCount;
                lastCount = 1;
                last = segment;
            }
            return count;
        }

        T last() {

            return last;
        }
        
        int lastCount() {
            return lastCount;
        }

        @Override
        public String toString() {
            return (lastCount == 0)
                    ? "SegmentBuffer(last=" + last + ')'
                    : "SegmentBuffer(FRESH)";
        }

        T last;
        int lastCount = 0;
    }
}
