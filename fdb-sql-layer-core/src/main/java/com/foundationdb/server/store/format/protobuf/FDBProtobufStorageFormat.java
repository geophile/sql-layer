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
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.server.store.format.StorageFormat;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.sql.parser.StorageFormatNode;

public class FDBProtobufStorageFormat extends StorageFormat<FDBProtobufStorageDescription>
{
    private static final String identifier = "protobuf";
    private FDBProtobufStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        CustomOptions.registerAllExtensions(registry.getExtensionRegistry());
        registry.registerStorageFormat(CommonProtobuf.protobufRow, identifier, FDBProtobufStorageDescription.class, new FDBProtobufStorageFormat());
    }

    public FDBProtobufStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, FDBProtobufStorageDescription storageDescription) {
        if (storageDescription == null) {
            storageDescription = new FDBProtobufStorageDescription(forObject, identifier);
        }
        storageDescription.readProtobuf(pbStorage.getExtension(CommonProtobuf.protobufRow));
        return storageDescription;
    }

    public FDBProtobufStorageDescription parseSQL(StorageFormatNode node, HasStorage forObject) {
        FDBProtobufStorageDescription storageDescription = new FDBProtobufStorageDescription(forObject,identifier);
        String singleTableOption = node.getOptions().get("no_group");
        boolean singleTable = (singleTableOption != null) && Boolean.valueOf(singleTableOption);
        String noTupleOption = node.getOptions().get("no_tuple");
        boolean noTuple = (noTupleOption != null) && Boolean.valueOf(noTupleOption);
        storageDescription.setFormatType(singleTable ?
                                         CommonProtobuf.ProtobufRowFormat.Type.SINGLE_TABLE :
                                         CommonProtobuf.ProtobufRowFormat.Type.GROUP_MESSAGE);
        storageDescription.setUsage(noTuple ?
                                    null :
                                    FDBProtobuf.TupleUsage.KEY_ONLY);
        return storageDescription;
    }

}
