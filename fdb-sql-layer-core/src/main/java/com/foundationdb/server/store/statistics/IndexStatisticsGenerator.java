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
package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.Index;
import com.foundationdb.server.store.statistics.histograms.Bucket;
import com.foundationdb.server.store.statistics.histograms.Sampler;
import com.foundationdb.util.Flywheel;

import java.util.ArrayList;
import java.util.List;

abstract class IndexStatisticsGenerator<K extends Comparable<? super K>, V>
{
    private final Flywheel<K> keysFlywheel;
    private final int columnCount;
    private final int singleColumnPosition; // -1 for multi-column
    private Sampler<K> keySampler;
    protected final Index index;
    protected final long timestamp;
    protected int rowCount;

    protected IndexStatisticsGenerator(Flywheel<K> keysFlywheel,
                                       Index index,
                                       int columnCount,
                                       int singleColumnPosition) {
        this.keysFlywheel = keysFlywheel;
        this.index = index;
        this.columnCount = columnCount;
        this.singleColumnPosition = singleColumnPosition;
        this.timestamp = System.currentTimeMillis();
        this.rowCount = 0;
    }


    //
    // IndexStatisticsGenerator, derived
    //

    protected abstract byte[] copyOfKeyBytes(K key);

    protected abstract Sampler<K> createKeySampler(int bucketCount, long distinctCount);

    protected final Flywheel<K> getKeysFlywheel() {
        return keysFlywheel;
    }

    protected final void loadKey(K key) {
        List<? extends K> recycles = keySampler.visit(key);
        rowCount++;
        for(K recycle : recycles) {
            keysFlywheel.recycle(recycle);
        }
    }


    //
    // IndexStatisticsGenerator, public
    //

    public abstract void visit(K key, V value);

    public final int columnCount() {
        return columnCount;
    }

    public final int rowCount() {
        return rowCount;
    }

    public void init(int bucketCount, long distinctCount) {
        this.keySampler = createKeySampler(bucketCount, distinctCount);
        keySampler.init();
    }

    public void finish(int bucketCount) {
        keySampler.finish();
    }

    public final void getIndexStatistics(IndexStatistics indexStatistics) {
        indexStatistics.setAnalysisTimestamp(timestamp);
        List<List<Bucket<K>>> segmentBuckets = keySampler.toBuckets();
        assert segmentBuckets.size() == columnCount
            : String.format("expected %s segments, saw %s: %s", columnCount, segmentBuckets.size(), segmentBuckets);
        for (int colCountSegment = 0; colCountSegment < columnCount; colCountSegment++) {
            List<Bucket<K>> segmentSamples = segmentBuckets.get(colCountSegment);
            int samplesCount = segmentSamples.size();
            List<HistogramEntry> entries = new ArrayList<>(samplesCount);
            for (Bucket<K> sample : segmentSamples) {
                K key = sample.value();
                byte[] keyBytes = copyOfKeyBytes(key);
                HistogramEntry entry = new HistogramEntry(
                    key.toString(),
                    keyBytes,
                    sample.getEqualsCount(),
                    sample.getLessThanCount(),
                    sample.getLessThanDistinctsCount()
                );
                entries.add(entry);
            }
            if (singleColumnPosition < 0) {
                indexStatistics.addHistogram(new Histogram(0, colCountSegment + 1, entries));
            } else if (singleColumnPosition > 0) {
                indexStatistics.addHistogram(new Histogram(singleColumnPosition, 1, entries));
            } else {
                // Single-column histogram for leading column is handled as a multi-column
                // histogram with column count = 1.
                assert false : "unnecesary sampler " + this;
            }
        }
    }
}
