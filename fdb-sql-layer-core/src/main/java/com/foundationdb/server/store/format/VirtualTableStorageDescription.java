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
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.qp.virtualadapter.VirtualScanFactory;
import com.foundationdb.server.error.StorageDescriptionInvalidException;

/** An in-memory system table. */
public class VirtualTableStorageDescription extends StorageDescription
{
    VirtualScanFactory virtualScanFactory;

    public VirtualTableStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public VirtualTableStorageDescription(HasStorage forObject,
                                          VirtualScanFactory virtualScanFactory,
                                          String storageFormat) {
        super(forObject, storageFormat);
        assert virtualScanFactory != null;
        this.virtualScanFactory = virtualScanFactory;
    }

    public VirtualTableStorageDescription(HasStorage forObject,
                                          VirtualTableStorageDescription other,
                                          String storageFormat) {
        super(forObject, other, storageFormat);
        this.virtualScanFactory = other.virtualScanFactory;
    }

    public VirtualScanFactory getVirtualScanFactory() {
        return virtualScanFactory;
    }

    public void setVirtualScanFactory(VirtualScanFactory virtualScanFactory) {
        this.virtualScanFactory = virtualScanFactory;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new VirtualTableStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        return new VirtualTableStorageDescription(forObject, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(CommonProtobuf.virtualTable,
                             CommonProtobuf.VirtualTableType.VIRTUAL_SCAN_FACTORY);
        writeUnknownFields(builder);
    }

    @Override
    public Object getUniqueKey() {
        return virtualScanFactory;
    }

    @Override
    public String getNameString() {
        return null;
    }

    @Override
    public boolean isVirtual() {
        return true;
    }

    @Override
    public void validate(AISValidationOutput output) {
        if (virtualScanFactory == null) {
            throw new StorageDescriptionInvalidException(object, "is missing factory");
        }
    }

}
