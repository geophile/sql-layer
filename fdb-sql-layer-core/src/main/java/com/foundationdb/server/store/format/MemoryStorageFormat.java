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
package com.foundationdb.server.store.format;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.MemoryProtobuf;

import java.util.UUID;

public class MemoryStorageFormat extends StorageFormat<MemoryStorageDescription>
{
    public final static String FORMAT_NAME = "memory";

    private MemoryStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        registry.registerStorageFormat(MemoryProtobuf.uuid,
                                       FORMAT_NAME,
                                       MemoryStorageDescription.class,
                                       new MemoryStorageFormat());
    }

    public MemoryStorageDescription readProtobuf(Storage pbStorage,
                                                 HasStorage forObject,
                                                 MemoryStorageDescription storageDescription) {
        if(storageDescription == null) {
            storageDescription = new MemoryStorageDescription(forObject, FORMAT_NAME);
        }
        String uuidStr = pbStorage.getExtension(MemoryProtobuf.uuid);
        if(uuidStr != null) {
            storageDescription.setUUID(UUID.fromString(uuidStr));
        }
        return storageDescription;
    }
}
