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
package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.google.protobuf.UnknownFieldSet;

public abstract class StorageDescription
{
    protected final HasStorage object;
    protected UnknownFieldSet unknownFields;
    protected final String storageFormat;

    protected StorageDescription(HasStorage object, String storageFormat) {
        assert(storageFormat != null): "StorageDescription created with null format";
        this.object = object;
        this.storageFormat = storageFormat;
    }
    
    protected StorageDescription(HasStorage object, StorageDescription other, String storageFormat) {
        this(object,storageFormat);
    }
    
    /** Get the AIS object for which this describes the storage. */
    public HasStorage getObject() {
        return object;
    }

    public String getSchemaName() {
        return object.getSchemaName();
    }

    /** Make a copy of this format (same tree name, for instance), but
     * pointing to a new object.
     */
    public abstract StorageDescription cloneForObject(HasStorage forObject);

    /** As {@link #cloneForObject(HasStorage)} but without internal state (e.g no tree name) **/
    public abstract StorageDescription cloneForObjectWithoutState(HasStorage forObject);
    
    /** Populate the extension fields of the <code>Storage</code>
     * field. */
    public abstract void writeProtobuf(Storage.Builder builder);

    public String getStorageFormat(){
        return storageFormat;
    }

    /** If there is a unique identifier for the storage "area"
     * described by this, return it, else <code>null</code>.
     */
    public abstract Object getUniqueKey();

    /** Get a string for printing the "location" of the storage
     * area. */
    public abstract String getNameString();

    /** Check that the <code>StorageDescription</code> has been filled
     * in completely and consistently before the AIS is frozen and
     * committed. */
    public abstract void validate(AISValidationOutput output);

    /** Does this describe something that lives in memory rather than
     * persistently? */
    public boolean isVirtual() {
        return false;
    }

    /** Does this description include unknown fields?
     * Such a <code>HasStorage</code> will save in the AIS but cannot be used.
     */
    public boolean hasUnknownFields() {
        return (unknownFields != null);
    }

    public void setUnknownFields(UnknownFieldSet unknownFields) {
        this.unknownFields = unknownFields;
    }

    public void writeUnknownFields(Storage.Builder builder) {
        if (unknownFields != null) {
            builder.mergeUnknownFields(unknownFields);
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getNameString()).append(" for ").append(object);
        if (unknownFields != null) {
            str.append(" with unknown fields ").append(unknownFields.asMap().keySet());
        }
        return str.toString();
    }

}
