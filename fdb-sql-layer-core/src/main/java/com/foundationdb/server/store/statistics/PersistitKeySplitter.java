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

import com.foundationdb.server.store.statistics.histograms.Splitter;
import com.foundationdb.util.Flywheel;
import com.persistit.Key;

import java.util.Arrays;
import java.util.List;

public class PersistitKeySplitter implements Splitter<Key> {
    private final List<Key> keys;
    private final Flywheel<Key> keysFlywheel;

    public PersistitKeySplitter(int columnCount, Flywheel<Key> keysFlywheel) {
        keys = Arrays.asList(new Key[columnCount]);
        this.keysFlywheel = keysFlywheel;
    }

    @Override
    public int segments() {
        return keys.size();
    }

    @Override
    public List<? extends Key> split(Key keyToSample) {
        Key prev = keyToSample;
        for (int i = keys.size() ; i > 0; i--) {
            Key truncatedKey = keysFlywheel.get();
            prev.copyTo(truncatedKey);
            truncatedKey.setDepth(i);
            keys.set(i-1 , truncatedKey);
            prev = truncatedKey;
        }
        return keys;
    }
}
