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

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.ProtobufReader;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.server.store.format.DummyStorageFormatRegistry;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;

public class UnknownStorageFormatTest
{
    private StorageFormatRegistry testFormatRegistry = DummyStorageFormatRegistry.create();
    private ByteBuffer bytes  = ByteBuffer.allocate(4096);
    private final static String identifier = "unknown";

    @Before
    public void saveWithExtension() {
        TestStorageFormatExtended.register(testFormatRegistry);

        AISBuilder aisb = new AISBuilder();
        Sequence sequence = aisb.sequence("test", "seq", 0, 1, 0, 1000, true);
        TestStorageDescriptionExtended storageDescription = new TestStorageDescriptionExtended(sequence, identifier);
        storageDescription.setStorageKey("KEY");
        storageDescription.setExtension("PLUS");
        assertTrue(isFullDescription(storageDescription));
        sequence.setStorageDescription(storageDescription);
        ProtobufWriter writer = new ProtobufWriter();
        writer.save(aisb.akibanInformationSchema());
        writer.serialize(bytes);
        bytes.flip();
    }

    @Test
    public void loadNormally() {
        TypesRegistry typesRegistry = TestTypesRegistry.MCOMPAT;
        Sequence sequence = loadSequence(typesRegistry, testFormatRegistry);
        assertNotNull(sequence);
        assertTrue(isFullDescription(sequence.getStorageDescription()));
    }

    @Test
    public void loadPartially() {
        TypesRegistry typesRegistry = TestTypesRegistry.MCOMPAT;
        StorageFormatRegistry newFormatRegistry = DummyStorageFormatRegistry.create();
        Sequence sequence = loadSequence(typesRegistry, newFormatRegistry);
        assertNotNull(sequence);
        assertFalse(isFullDescription(sequence.getStorageDescription()));
        assertTrue(isPartialDescription(sequence.getStorageDescription()));
    }

    @Test
    public void reloadNormally() {
        TypesRegistry typesRegistry = TestTypesRegistry.MCOMPAT;
        StorageFormatRegistry newFormatRegistry = DummyStorageFormatRegistry.create();
        AkibanInformationSchema ais = new AkibanInformationSchema();
        ProtobufReader reader = new ProtobufReader(typesRegistry, newFormatRegistry, ais);
        reader.loadBuffer(bytes);
        reader.loadAIS();
        bytes.flip();
        ProtobufWriter writer = new ProtobufWriter();
        writer.save(ais);
        writer.serialize(bytes);
        bytes.flip();
        Sequence sequence = loadSequence(typesRegistry, testFormatRegistry);
        assertNotNull(sequence);
        assertTrue(isFullDescription(sequence.getStorageDescription()));
    }

    protected Sequence loadSequence(TypesRegistry typesRegistry, StorageFormatRegistry storageFormatRegistry) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        ProtobufReader reader = new ProtobufReader(typesRegistry, storageFormatRegistry, ais);
        reader.loadBuffer(bytes);
        reader.loadAIS();
        return ais.getSequence(new TableName("test", "seq"));
 }

    protected boolean isFullDescription(StorageDescription storageDescription) {
        return (isPartialDescription(storageDescription) &&
                (storageDescription instanceof TestStorageDescriptionExtended) &&
                "PLUS".equals(((TestStorageDescriptionExtended)storageDescription).getExtension()));
    }

    protected boolean isPartialDescription(StorageDescription storageDescription) {
        return ((storageDescription instanceof TestStorageDescription) &&
                "KEY".equals(((TestStorageDescription)storageDescription).getStorageKey()));
    }

}
