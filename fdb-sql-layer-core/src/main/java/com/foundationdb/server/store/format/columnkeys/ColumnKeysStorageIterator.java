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
package com.foundationdb.server.store.format.columnkeys;

import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.FDBStoreDataIterator;
import com.foundationdb.KeyValue;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class ColumnKeysStorageIterator extends FDBStoreDataIterator
{
    private final AsyncIterator<KeyValue> underlying;
    private final int limit;
    private KeyValue pending;
    private int count;

    public ColumnKeysStorageIterator(FDBStoreData storeData, 
                                     AsyncIterator<KeyValue> underlying,
                                     int limit) {
        super(storeData);
        this.underlying = underlying;
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        try {
        return (((pending != null) || underlying.hasNext()) &&
                ((limit <= 0) || (count < limit)));
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(storeData.session, e);
        }
    }

    /**
     * Do next processing, but also duplicate key elimination.
     * That is if the underlying iterator over the keys would return:
     * 'a', 'a', 'b', 'b', 'b', 'c'. 
     * this will return 'a', 'b', 'c'. 
     * pending holds the next key in the sequence, so when completed
     * reading the 'a's, pending holds 'b'. 
     */
    @Override
    public Void next() {
        Map<String,Object> value = new HashMap<>();
        Tuple2 lastKey = null;
        while (true) {
            KeyValue kv;
            if (pending != null) {
                kv = pending;
                pending = null;
            } else {
                try {
                    if (underlying.hasNext()) {
                        kv = underlying.next();
                    } else {
                        break;
                    }
                } catch (RuntimeException e) {
                    throw FDBAdapter.wrapFDBException(storeData.session, e);
                }
            }
            
            Tuple2 key = Tuple2.fromBytes(kv.getKey());
            String name = key.getString(key.size() - 1);
            key = key.popBack();
            if (lastKey == null) {
                lastKey = key;
            }
            else if (!key.equals(lastKey)) {
                pending = kv;
                break;
            }
            value.put(name, Tuple2.fromBytes(kv.getValue()).get(0));
        }
        storeData.rawKey = lastKey.pack();
        storeData.otherValue = value;
        count++;
        return null;
    }

    @Override
    public void close() {
        underlying.dispose();
    }
}
