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

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.qp.virtualadapter.VirtualScanFactory;

import java.util.Map;

public class VirtualTableStorageFormat extends StorageFormat<VirtualTableStorageDescription>
{
    private final Map<TableName,VirtualScanFactory> virtualScanFactories;

    final static String identifier = "virtual";

    private VirtualTableStorageFormat(Map<TableName, VirtualScanFactory> virtualScanFactories) {
        this.virtualScanFactories = virtualScanFactories;
    }

    public static void register(StorageFormatRegistry registry, Map<TableName,VirtualScanFactory> virtualScanFactories) {
        registry.registerStorageFormat(CommonProtobuf.virtualTable, null, VirtualTableStorageDescription.class, new VirtualTableStorageFormat(virtualScanFactories));
    }

    public VirtualTableStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, VirtualTableStorageDescription storageDescription) {
        switch (pbStorage.getExtension(CommonProtobuf.virtualTable)) {
        case VIRTUAL_SCAN_FACTORY:
            if (storageDescription == null) {
                storageDescription = new VirtualTableStorageDescription(forObject, identifier);
            }
            storageDescription.setVirtualScanFactory(virtualScanFactories.get(((Group)forObject).getName()));
            break;
        }
        return storageDescription;
    }
}
