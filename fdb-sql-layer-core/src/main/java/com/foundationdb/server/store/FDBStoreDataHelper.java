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

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;
import com.persistit.Key;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FDBStoreDataHelper
{
    private FDBStoreDataHelper() {
    }

    public static byte[] prefixBytes(FDBStoreData storeData) {
        return prefixBytes(storeData.storageDescription);
    }

    public static byte[] prefixBytes(HasStorage object) {
        return prefixBytes((FDBStorageDescription)object.getStorageDescription());
    }

    public static byte[] prefixBytes(FDBStorageDescription storageDescription) {
        return storageDescription.getPrefixBytes();
    }

    public static void unpackKey(FDBStoreData storeData) {
        unpackTuple(storeData.storageDescription, storeData.persistitKey,
                    storeData.rawKey);
    }

    public static void unpackTuple(HasStorage object, Key key, byte[] tupleBytes) {
        unpackTuple((FDBStorageDescription)object.getStorageDescription(),
                    key, tupleBytes);
    }

    public static void unpackTuple(FDBStorageDescription storageDescription, Key key, byte[] tupleBytes) {
        byte[] treeBytes = prefixBytes(storageDescription);
        // TODO: Use fromBytes(byte[],int,int) when available.
        Tuple2 tuple = Tuple2.fromBytes(Arrays.copyOfRange(tupleBytes, treeBytes.length, tupleBytes.length));
        storageDescription.getTupleKey(tuple, key);
    }

    public static byte[] packKey(FDBStoreData storeData) {
        return packKey(storeData, null);
    }

    public static byte[] packKey(FDBStoreData storeData, Key.EdgeValue edge) {
        storeData.rawKey = packedTuple(storeData.storageDescription, 
                                       storeData.persistitKey, edge, storeData.nudgeDir);
        return storeData.rawKey;
    }

    public static byte[] packedTuple(HasStorage object, Key key) {
        return packedTuple(object, key, null);
    }

    public static byte[] packedTuple(HasStorage object, Key key, Key.EdgeValue edge) {
        return packedTuple((FDBStorageDescription)object.getStorageDescription(), key, edge, null);
    }

    public static byte[] packedTuple(FDBStorageDescription storageDescription, Key key) {
        return packedTuple(storageDescription, key, null, null);
    }

    public static byte[] packedTuple(FDBStorageDescription storageDescription, Key key, Key.EdgeValue edge, FDBStoreData.NudgeDir nudged) {
        byte[] treeBytes = prefixBytes(storageDescription);
        if (edge != null) {
            // TODO: Could eliminate new key if callers didn't rely on key state outside getEncodedSize()
            // (see checkUniqueness() in FDBStore).
            Key nkey = new Key(null, key.getEncodedSize() + 1);
            key.copyTo(nkey);
            key = nkey;
            key.append(edge);
        }
        byte[] keyBytes = storageDescription.getKeyBytes(key, nudged);
        return ByteArrayUtil.join(treeBytes, keyBytes);
    }

    public static void unpackValue(FDBStoreData storeData) {
        storeData.persistitValue.clear();
        storeData.persistitValue.putEncodedBytes(storeData.rawValue, 0, storeData.rawValue.length);
    }

    private static final Logger logger = LoggerFactory.getLogger(FDBStoreDataHelper.class);
}
