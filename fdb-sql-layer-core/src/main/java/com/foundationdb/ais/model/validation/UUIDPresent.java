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
import com.foundationdb.server.error.AISValidationException;

/** Every table and every column has a UUID. */
public class UUIDPresent implements AISValidation
{
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(Table t : ais.getTables().values()) {
            if(t.getUuid() == null) {
                output.reportFailure(new AISValidationFailure(missingTable(t)));
            }
            for(Column c: t.getColumnsIncludingInternal()) {
                if(c.getUuid() == null) {
                    output.reportFailure(new AISValidationFailure(missingColumn(t, c)));
                }
            }
        }
    }

    private AISValidationException missingTable(Table t) {
        return new AISValidationException(String.format("Table %s missing UUID", t.getName()));
    }

    private AISValidationException missingColumn(Table t, Column c) {
        return new AISValidationException(String.format("Column %s.%s missing UUID", t.getName(), c.getName()));
    }
}
