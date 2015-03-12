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
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.FDBProtobuf.TupleUsage;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.store.FDBNameGenerator;
import com.foundationdb.server.store.format.columnkeys.ColumnKeysStorageFormat;
import com.foundationdb.server.store.format.protobuf.FDBProtobufStorageFormat;
import com.foundationdb.server.store.format.tuple.TupleStorageDescription;
import com.foundationdb.server.store.format.tuple.TupleStorageFormat;

public class FDBStorageFormatRegistry extends StorageFormatRegistry
{
    public FDBStorageFormatRegistry(ConfigurationService configService) {
        super(configService);
    }

    @Override
    public void registerStandardFormats() {
        FDBStorageFormat.register(this);
        TupleStorageFormat.register(this);
        FDBProtobufStorageFormat.register(this);
        ColumnKeysStorageFormat.register(this);
        super.registerStandardFormats();
    }

    @Override
    public StorageDescription getDefaultStorageDescription(HasStorage object) {
        StorageDescription sd = super.getDefaultStorageDescription(object);
        if(sd instanceof TupleStorageDescription) {
            TupleStorageDescription tsd = (TupleStorageDescription)sd;
            if(object instanceof Group) {
                tsd.setUsage(TupleUsage.KEY_AND_ROW);
            } else {
                tsd.setUsage(TupleUsage.KEY_ONLY);
            }
        }
        return sd;
    }

    public boolean isDescriptionClassAllowed(Class<? extends StorageDescription> descriptionClass) {
        return (super.isDescriptionClassAllowed(descriptionClass) ||
                FDBStorageDescription.class.isAssignableFrom(descriptionClass));
    }

    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        super.finishStorageDescription(object, nameGenerator);
        assert object.getStorageDescription() != null;
        if (object.getStorageDescription() instanceof FDBStorageDescription) {
            FDBStorageDescription storageDescription = 
                    (FDBStorageDescription)object.getStorageDescription();
            if (storageDescription.getPrefixBytes() == null) {
                storageDescription.setPrefixBytes(generatePrefixBytes(object, (FDBNameGenerator)nameGenerator));
            }
        }
    }

    protected byte[] generatePrefixBytes(HasStorage object, FDBNameGenerator nameGenerator) {
        if (object instanceof Index) {
            return nameGenerator.generateIndexPrefixBytes((Index)object);
        }
        else if (object instanceof Group) {
            TableName name = ((Group)object).getName();
            return nameGenerator.generateGroupPrefixBytes(name.getSchemaName(), name.getTableName());
        }
        else if (object instanceof Sequence) {
            return nameGenerator.generateSequencePrefixBytes((Sequence)object);
        }
        else {
            throw new IllegalArgumentException(object.toString());
        }
    }

}
