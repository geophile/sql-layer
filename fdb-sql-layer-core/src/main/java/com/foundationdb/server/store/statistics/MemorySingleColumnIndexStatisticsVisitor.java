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

import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.statistics.histograms.Sampler;
import com.persistit.Key;

import java.util.Map;
import java.util.TreeMap;

public class MemorySingleColumnIndexStatisticsVisitor extends IndexStatisticsGenerator<Key,byte[]>
{
    private final Key extractKey = new Key(null, Key.MAX_KEY_LENGTH);
    private final Map<Key,int[]> countMap = new TreeMap<>(); // Is ordered required?
    private final int field;

    public MemorySingleColumnIndexStatisticsVisitor(KeyCreator keyCreator, IndexColumn indexColumn)
    {
        super(new PersistitKeyFlywheel(keyCreator), indexColumn.getIndex(), 1, indexColumn.getPosition());
        this.field = indexColumn.getPosition();
    }


    @Override
    public void init(int bucketCount, long expectedRowCount) {
        // Does not do generator init until finish because just
        // buffering counts in this pass.
    }

    @Override
    public void finish(int bucketCount) {
        // Now init the generator and replay the accumulated counts.
        super.init(bucketCount, rowCount);
        try {
            for(Map.Entry<Key,int[]> entry : countMap.entrySet()) {
                int keyCount = entry.getValue()[0];
                for(int i = 0; i < keyCount; ++i) {
                    loadKey(entry.getKey());
                }
            }
        } finally {
            super.finish(bucketCount);
        }
    }

    @Override
    public void visit(Key key, byte[] value) {
        key.indexTo(field);
        extractKey.clear().appendKeySegment(key);
        int[] curCount = countMap.get(extractKey);
        if(curCount == null) {
            Key storedKey = new Key(extractKey);
            curCount = new int[1];
            countMap.put(storedKey, curCount);
        }
        curCount[0] += 1;
        rowCount++;
    }

    @Override
    public Sampler<Key> createKeySampler(int bucketCount, long distinctCount) {
        return new Sampler<>(
            new PersistitKeySplitter(columnCount(), getKeysFlywheel()),
            bucketCount,
            distinctCount,
            getKeysFlywheel()
        );
    }

    @Override
    protected byte[] copyOfKeyBytes(Key key) {
        byte[] copy = new byte[key.getEncodedSize()];
        System.arraycopy(key.getEncodedBytes(), 0, copy, 0, copy.length);
        return copy;
    }
}
