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
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.error.IndexColumnIsPartialException;
import com.foundationdb.server.types.service.TestTypesRegistry;
import org.junit.Test;

import java.util.Collections;

public class IndexColumnIsNotPartialTest {
    private final static String SCHEMA = "test";
    private final static String TABLE = "t";

    private static AkibanInformationSchema createAIS(long fullLen, Integer indexedLength) {
        TestAISBuilder builder = new TestAISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "v", 0, "MCOMPAT", "VARCHAR", fullLen, null, false);
        builder.index(SCHEMA, TABLE, "v");
        builder.indexColumn(SCHEMA, TABLE, "v", "v", 0, true, indexedLength);
        builder.createGroup(TABLE, SCHEMA);
        builder.addTableToGroup(TABLE, SCHEMA, TABLE);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        return builder.akibanInformationSchema();
    }

    private static void validate(AkibanInformationSchema ais) {
        ais.validate(Collections.<AISValidation>singleton(new IndexColumnIsNotPartial())).throwIfNecessary();
    }

    @Test
    public void nullIndexedLengthIsValid() {
        validate(createAIS(32, null));
    }

    @Test(expected=IndexColumnIsPartialException.class)
    public void fullLengthIsInvalid() {
        validate(createAIS(32, 32));
    }

    @Test(expected=IndexColumnIsPartialException.class)
    public void partialLengthIsInvalid() {
        validate(createAIS(32, 16));
    }
}
