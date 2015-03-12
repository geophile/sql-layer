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
package com.foundationdb.server.store.format.tuple;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.server.store.format.StorageFormat;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.sql.parser.StorageFormatNode;

public class TupleStorageFormat extends StorageFormat<TupleStorageDescription>
{
    private final static String storageFormat = "tuple";

    private TupleStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        registry.registerStorageFormat(FDBProtobuf.tupleUsage, storageFormat, TupleStorageDescription.class, new TupleStorageFormat());
    }

    public TupleStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, TupleStorageDescription storageDescription) {
        if (storageDescription == null) {
            storageDescription = new TupleStorageDescription(forObject, storageFormat);
        }
        storageDescription.setUsage(pbStorage.getExtension(FDBProtobuf.tupleUsage));
        return storageDescription;
    }

    public TupleStorageDescription parseSQL(StorageFormatNode node, HasStorage forObject) {
        TupleStorageDescription storageDescription = new TupleStorageDescription(forObject, storageFormat);
        boolean keyOnly = true;
        if (forObject instanceof Group) {
            String keyOnlyOption = node.getOptions().get("key_only");
            keyOnly = (keyOnlyOption != null) && Boolean.valueOf(keyOnlyOption);
        }
        storageDescription.setUsage(keyOnly ?
                                    FDBProtobuf.TupleUsage.KEY_ONLY :
                                    FDBProtobuf.TupleUsage.KEY_AND_ROW);
        return storageDescription;
    }

}
