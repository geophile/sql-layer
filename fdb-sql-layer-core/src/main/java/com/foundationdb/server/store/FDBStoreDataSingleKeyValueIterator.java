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
package com.foundationdb.server.store;

import com.foundationdb.async.Future;
import com.foundationdb.qp.storeadapter.FDBAdapter;

/**
 * Substiture for {@link FDBStoreDataKeyValueIterator} that uses a single value future
 * and emits that {@link KeyValue} if present.
 */
public class FDBStoreDataSingleKeyValueIterator extends FDBStoreDataIterator
{
    private final byte[] key;
    private Future<byte[]> futureValue;
    private byte[] value;

    public FDBStoreDataSingleKeyValueIterator(FDBStoreData storeData,
                                              byte[] key,
                                              Future<byte[]> value) {
        super(storeData);
        this.key = key;
        this.futureValue = value;
    }

    @Override
    public boolean hasNext() {
        if (futureValue == null) {
            return false;
        }
        try {
            value = futureValue.get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(storeData.session, e);
        }
        futureValue = null;
        return (value != null);
    }

    @Override
    public Void next() {
        storeData.rawKey = key;
        storeData.rawValue = value;
        return null;
    }

    @Override
    public void close() {
        if (futureValue != null) {
            futureValue.dispose();
        }
    }
}
