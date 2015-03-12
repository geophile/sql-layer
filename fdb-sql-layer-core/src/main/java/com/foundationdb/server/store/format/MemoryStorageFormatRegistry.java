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
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.store.format.protobuf.MemoryProtobufStorageFormat;

import java.util.UUID;

public class MemoryStorageFormatRegistry extends StorageFormatRegistry
{
    public MemoryStorageFormatRegistry(ConfigurationService configService) {
        super(configService);
    }

    @Override
    public void registerStandardFormats() {
        MemoryStorageFormat.register(this);
        MemoryProtobufStorageFormat.register(this);
        super.registerStandardFormats();
    }

    @Override
    void getDefaultDescriptionConstructor()
    {}

    // Note: Overrides any configured
    @Override
    public StorageDescription getDefaultStorageDescription(HasStorage object) {
        return new MemoryStorageDescription(object, MemoryStorageFormat.FORMAT_NAME);
    }

    public boolean isDescriptionClassAllowed(Class<? extends StorageDescription> descriptionClass) {
        return (super.isDescriptionClassAllowed(descriptionClass) ||
               MemoryStorageDescription.class.isAssignableFrom(descriptionClass));
    }

    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        super.finishStorageDescription(object, nameGenerator);
        assert object.getStorageDescription() != null;
        if(object.getStorageDescription() instanceof MemoryStorageDescription) {
            MemoryStorageDescription storageDescription = (MemoryStorageDescription)object.getStorageDescription();
            if(storageDescription.getUUID() == null) {
                storageDescription.setUUID(UUID.randomUUID());
            }
        }
    }
}
