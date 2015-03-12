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
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.TestProtobuf;
import com.foundationdb.server.error.StorageDescriptionInvalidException;

public class TestStorageDescriptionExtended extends TestStorageDescription
{
    private String extension;

    public TestStorageDescriptionExtended(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public TestStorageDescriptionExtended(HasStorage forObject, TestStorageDescriptionExtended other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.extension = other.extension;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new TestStorageDescriptionExtended(forObject, this, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        super.writeProtobuf(builder);
        builder.setExtension(TestProtobuf.storageExtension, extension);
        writeUnknownFields(builder);
    }

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }
}
