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
package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.server.store.format.StorageFormat;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.sql.parser.StorageFormatNode;

public class MemoryProtobufStorageFormat extends StorageFormat<MemoryProtobufStorageDescription>
{
    private final static String FORMAT_NAME = "protobuf";

    private MemoryProtobufStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        CustomOptions.registerAllExtensions(registry.getExtensionRegistry());
        registry.registerStorageFormat(CommonProtobuf.protobufRow,
                                       FORMAT_NAME,
                                       MemoryProtobufStorageDescription.class,
                                       new MemoryProtobufStorageFormat());
    }

    public MemoryProtobufStorageDescription readProtobuf(Storage pbStorage,
                                                          HasStorage forObject,
                                                          MemoryProtobufStorageDescription storageDescription) {
        if(storageDescription == null) {
            storageDescription = new MemoryProtobufStorageDescription(forObject, FORMAT_NAME);
        }
        storageDescription.readProtobuf(pbStorage.getExtension(CommonProtobuf.protobufRow));
        return storageDescription;
    }


    public MemoryProtobufStorageDescription parseSQL(StorageFormatNode node, HasStorage forObject) {
        MemoryProtobufStorageDescription storageDescription = new MemoryProtobufStorageDescription(forObject, FORMAT_NAME);
        String singleTableOption = node.getOptions().get("no_group");
        boolean singleTable = (singleTableOption != null) && Boolean.valueOf(singleTableOption);
        storageDescription.setFormatType(singleTable ?
                                             CommonProtobuf.ProtobufRowFormat.Type.SINGLE_TABLE :
                                             CommonProtobuf.ProtobufRowFormat.Type.GROUP_MESSAGE);
        return storageDescription;
    }
}
