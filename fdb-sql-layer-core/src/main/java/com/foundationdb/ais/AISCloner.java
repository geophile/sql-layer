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
package com.foundationdb.ais;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.protobuf.AISProtobuf;
import com.foundationdb.ais.protobuf.ProtobufReader;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.service.TypesRegistry;

public class AISCloner {
    private final TypesRegistry typesRegistry;
    private final StorageFormatRegistry storageFormatRegistry;

    public AISCloner(TypesRegistry typesRegistry, StorageFormatRegistry storageFormatRegistry) {
        this.typesRegistry = typesRegistry;
        this.storageFormatRegistry = storageFormatRegistry;
    }

    public AkibanInformationSchema clone(AkibanInformationSchema ais) {
        return clone(ais, ProtobufWriter.ALL_SELECTOR);
    }

    public TypesRegistry getTypesRegistry() {
        return typesRegistry;
    }

    public StorageFormatRegistry getStorageFormatRegistry() {
        return storageFormatRegistry;
    }

    public AkibanInformationSchema clone(AkibanInformationSchema ais, ProtobufWriter.WriteSelector selector) {
        AkibanInformationSchema newAIS = new AkibanInformationSchema();
        clone(newAIS, ais, selector);
        return newAIS;
    }

    public void clone(AkibanInformationSchema destAIS, AkibanInformationSchema srcAIS, ProtobufWriter.WriteSelector selector) {
        ProtobufWriter writer = new ProtobufWriter(selector);
        AISProtobuf.AkibanInformationSchema pbAIS = writer.save(srcAIS);
        ProtobufReader reader = new ProtobufReader(typesRegistry, storageFormatRegistry, destAIS, pbAIS.toBuilder());
        reader.loadAIS();
    }
}
