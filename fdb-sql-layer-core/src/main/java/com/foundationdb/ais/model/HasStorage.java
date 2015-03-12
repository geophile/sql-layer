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

public abstract class HasStorage
{
    protected StorageDescription storageDescription;

    public StorageDescription getStorageDescription() {
        return storageDescription;
    }

    public void setStorageDescription(StorageDescription storageDescription) {
        this.storageDescription = storageDescription;
    }

    public void copyStorageDescription(HasStorage fromObject) {
        this.storageDescription = 
            (fromObject.storageDescription != null) ?
            fromObject.storageDescription.cloneForObject(this) :
            null;
    }
    
    public abstract AkibanInformationSchema getAIS();

    public abstract String getTypeString();

    public abstract String getNameString();

    public abstract String getSchemaName();

    public Object getStorageUniqueKey() {
        return (storageDescription != null) ? storageDescription.getUniqueKey() : null;
    }

    public String getStorageNameString() {
        return (storageDescription != null) ? storageDescription.getNameString() : null;
    }

    @Override
    public String toString()
    {
        return getTypeString() + "(" + getNameString() + ")";
    }
}
