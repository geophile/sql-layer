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

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.format.MemoryStorageDescription;
import com.persistit.Key;
import com.persistit.Value;

import java.util.Iterator;
import java.util.Map.Entry;

public class MemoryStoreData
{
    public final Session session;
    public final MemoryStorageDescription storageDescription;
    public final Key persistitKey;
    public final Key endKey;
    public Value persistitValue;
    public byte[] rawKey;
    public byte[] rawValue;
    public Row row;
    public Iterator<Entry<byte[],byte[]>> iterator;

    public MemoryStoreData(Session session, MemoryStorageDescription storageDescription, Key persistitKey, Key endKey) {
        this.session = session;
        this.storageDescription = storageDescription;
        this.persistitKey = persistitKey;
        this.endKey = endKey;
    }

    public boolean next() {
        if(iterator.hasNext()) {
            Entry<byte[], byte[]> entry = iterator.next();
            rawKey = entry.getKey();
            rawValue = entry.getValue();
            return true;
        } else {
            return false;
        }
    }

    public void closeIterator() {
        if(iterator != null) {
            iterator = null;
        }
    }
}
