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
package com.foundationdb.ais.model.validation;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class UUIDPresentTest
{
    private static Collection<AISValidationFailure> validate(AkibanInformationSchema ais) {
        return ais.validate(Collections.singleton(new UUIDPresent())).failures();
    }

    private static AkibanInformationSchema build() {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        return AISBBasedBuilder.create("test", typesTranslator).table("t").colInt("id").pk("id").unvalidatedAIS();
    }

    @Test
    public void missingFromTable() {
        AkibanInformationSchema ais = build();
        Table t = ais.getTable("test", "t");
        t.setUuid(null);
        t.getColumn("id").setUuid(UUID.randomUUID());
        assertEquals("failures", 1, validate(ais).size());
    }

    @Test
    public void missingFromColumn() {
        AkibanInformationSchema ais = build();
        Table t = ais.getTable("test", "t");
        t.setUuid(UUID.randomUUID());
        t.getColumn("id").setUuid(null);
        assertEquals("failures", 1, validate(ais).size());
    }
}
