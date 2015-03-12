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

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.server.store.format.StorageFormat;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.sql.parser.StorageFormatNode;

public class ColumnKeysStorageFormat extends StorageFormat<ColumnKeysStorageDescription>
{
    public final static String storageFormat = "column_keys";

    private ColumnKeysStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        registry.registerStorageFormat(FDBProtobuf.columnKeys, storageFormat, ColumnKeysStorageDescription.class, new ColumnKeysStorageFormat());
    }

    public ColumnKeysStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, ColumnKeysStorageDescription storageDescription) {
        if (storageDescription == null) {
            storageDescription = new ColumnKeysStorageDescription(forObject, storageFormat);
        }
        // no options yet
        return storageDescription;
    }

    public ColumnKeysStorageDescription parseSQL(StorageFormatNode node, HasStorage forObject) {
        ColumnKeysStorageDescription storageDescription = new ColumnKeysStorageDescription(forObject, storageFormat);
        // no options yet
        return storageDescription;
    }
}
