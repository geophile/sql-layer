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
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.ColumnSizeMismatchException;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.util.JUnitUtils;
import org.junit.Test;

import java.util.Collections;

public class ColumnMaxAndPrefixSizesMatchTest {
    private final static String SCHEMA = "test";
    private final static String TABLE = "t";

    private static AkibanInformationSchema createAIS(Long maxStorage, Integer prefix) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        Table table = Table.create(ais, SCHEMA, TABLE, 1);
        Column.create(table, "id", 0, TestTypesRegistry.MCOMPAT.getTypeClass("MCOMPAT", "BIGINT").instance(false), maxStorage, prefix);
        return ais;
    }

    private static void validate(AkibanInformationSchema ais) {
        ais.validate(Collections.<AISValidation>singleton(new ColumnMaxAndPrefixSizesMatch())).throwIfNecessary();
    }

    @Test
    public void nulls() {
        validate(createAIS(null, null));
    }

    @Test
    public void correct() {
        validate(createAIS(8L, 0));
    }

    @Test(expected=ColumnSizeMismatchException.class)
    public void wroteMaxIsError() {
        validate(createAIS(50L, 0));
    }

    @Test(expected=ColumnSizeMismatchException.class)
    public void wrongPrefixIsError() {
        validate(createAIS(8L, 50));
    }

    @Test
    public void wrongStorageAndPrefixIsError() {
        JUnitUtils.expectMultipleCause(
            new Runnable() {
                @Override
                public void run() {
                    validate(createAIS(50L, 50));
                }
            },
            ColumnSizeMismatchException.class, // maxStorage
            ColumnSizeMismatchException.class  // prefix
        );
    }
}
