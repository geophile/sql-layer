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

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.server.types.service.TestTypesRegistry;

import java.util.HashSet;
import java.util.Set;

public class DummyStorageFormatRegistry extends StorageFormatRegistry
{
    private final Set<String> generated;
    private final static String identifier = "dummy";

    public DummyStorageFormatRegistry() {
        super("dummy");
        this.generated = new HashSet<>();
    }

    @Override
    public void registerStandardFormats() {
        TestStorageFormat.register(this);
        super.registerStandardFormats();
    }

    @Override
    void getDefaultDescriptionConstructor() {}

    @Override
    public StorageDescription getDefaultStorageDescription(HasStorage object) {
        return null;
    }

    @Override
    public boolean isDescriptionClassAllowed(Class<? extends StorageDescription> descriptionClass) {
        return true;
    }

    public static StorageFormatRegistry create() {
        StorageFormatRegistry result = new DummyStorageFormatRegistry();
        result.registerStandardFormats();
        return result;
    }

    /** Convenience to make an AISCloner using the dummy. */
    public static AISCloner aisCloner() {
        return new AISCloner(TestTypesRegistry.MCOMPAT, create());
    }
    
    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        super.finishStorageDescription(object, nameGenerator);
        if (object.getStorageDescription() == null) {
            object.setStorageDescription(new TestStorageDescription(object, generateStorageKey(object), identifier));
        }
        assert object.getStorageDescription() != null;
    }

    protected String generateStorageKey(HasStorage object) {
        final String proposed;
        if (object instanceof Index) {
            proposed = ((Index)object).getIndexName().toString();
        }
        else if (object instanceof Group) {
            proposed = ((Group)object).getName().toString();
        }
        else if (object instanceof Sequence) {
            proposed =  ((Sequence)object).getSequenceName().toString();
        }
        else {
            throw new IllegalArgumentException(object.toString());
        }
        return DefaultNameGenerator.makeUnique(generated, proposed, Integer.MAX_VALUE);
    }
}
