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

import java.util.LinkedList;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.collation.AkCollatorFactory.Mode;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;
import com.foundationdb.server.error.UnsupportedCollationException;

public class AISCollationValidationTest {
    private LinkedList<AISValidation> validations;

    @Before
    public void createValidations() {
        validations = new LinkedList<>();
        validations.add(AISValidations.COLLATION_SUPPORTED);
    }

    private final TypesRegistry typesRegistry = TestTypesRegistry.MCOMPAT;

    @Test
    public void testSupportedCollation() {
        final TestAISBuilder builder = new TestAISBuilder(typesRegistry);
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, "MCOMPAT", "VARCHAR", 16L, true, null, "en_us");
        builder.basicSchemaIsComplete();
        Assert.assertEquals("Expect no validation failure for supported collation", 0, builder
                .akibanInformationSchema().validate(validations).failures().size());
    }

    @Test
    public void testUnsupportedCollationStrictMode() {
        Mode save = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(Mode.STRICT);
            final TestAISBuilder builder = new TestAISBuilder(typesRegistry);
            builder.table("test", "t1");
            builder.column("test", "t1", "c1", 0, "MCOMPAT", "VARCHAR", 16L, true, null, "fricostatic_sengalese_ci");
            builder.basicSchemaIsComplete();
            Assert.assertEquals("Expect validation failure on invalid collation", 1, builder.akibanInformationSchema()
                    .validate(validations).failures().size());
        } catch (UnsupportedCollationException ex) {
            // Okay if thrown earlier.
        } finally {
            AkCollatorFactory.setCollationMode(save);
        }
    }

    @Test
    public void testUnsupportedCollationLooseMode() {
        Mode save = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(Mode.LOOSE);
            final TestAISBuilder builder = new TestAISBuilder(typesRegistry);
            builder.table("test", "t1");
            builder.column("test", "t1", "c1", 0, "MCOMPAT", "VARCHAR", 16L, true, null, "fricostatic_sengalese_ci");
            builder.basicSchemaIsComplete();
            Assert.assertEquals("Expect no validation failure in loose mode", 0, builder.akibanInformationSchema()
                    .validate(validations).failures().size());
        } finally {
            AkCollatorFactory.setCollationMode(save);
        }
    }

}
