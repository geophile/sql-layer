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
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import java.io.File;

/** A full text index saved in the local file system. */
public class FullTextIndexFileStorageDescription extends StorageDescription
{
    File path;

    public FullTextIndexFileStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public FullTextIndexFileStorageDescription(HasStorage forObject, File path, String storageFormat) {
        super(forObject, storageFormat);
        this.path = path;
    }

    public FullTextIndexFileStorageDescription(HasStorage forObject, FullTextIndexFileStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.path = other.path;
    }

    public File getPath() {
        return path;
    }

    public File mergePath(File basepath) {
        if (path == null)
            return null;
        else if (path.isAbsolute())
            return path;
        else
            return new File(basepath, path.getPath());
    }

    public void setPath(File path) {
        this.path = path;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new FullTextIndexFileStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        return new FullTextIndexFileStorageDescription(forObject, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(CommonProtobuf.fullTextIndexPath, path.getPath());
        writeUnknownFields(builder);
    }

    @Override
    public Object getUniqueKey() {
        return path;
    }

    @Override
    public String getNameString() {
        return (path != null) ? path.getPath() : null;
    }

    @Override
    public void validate(AISValidationOutput output) {
        if (path == null) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is missing path")));
        }
    }

}
