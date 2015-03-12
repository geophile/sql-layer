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

import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.persistit.Key;
import com.persistit.Value;

/**
 * State used for traversing / modifying FDB storage.
 */
public class FDBStoreData {
    public static enum NudgeDir {
        LEFT,
        RIGHT,
        DEEPER,
    }

    public final FDBStorageDescription storageDescription;
    public final Key persistitKey;
    public final Session session;
    public byte[] rawKey;
    public byte[] rawValue;
    public Value persistitValue;
    public Object otherValue;
    public FDBStoreDataIterator iterator;
    // What way, if any, persistitKey has been nudged
    public NudgeDir nudgeDir;
    public Key endKey;
    public boolean exactEnd;
    
    public FDBStoreData(Session session, FDBStorageDescription storageDescription, Key persistitKey, Key endKey) {
        this.storageDescription = storageDescription;
        this.persistitKey = persistitKey;
        this.endKey = endKey;
        this.session = session;
    }

    public boolean next() {
        if (iterator.hasNext()) {
            iterator.next();
            return true;
        }
        else {
            return false;
        }
    }

    public void closeIterator() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
    }
}
