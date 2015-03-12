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
import com.foundationdb.server.error.AISValidationException;

/** Every table has an ordinal. If a table is a child, it's ordinal is greater than the parent's. */
public class OrdinalOrdering implements AISValidation
{
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(Table t : ais.getTables().values()) {
            if(t.getOrdinal() == null) {
                output.reportFailure(new AISValidationFailure(noOrdinal(t)));
            } else {
                Table p = t.getParentTable();
                if((p != null) && (p.getOrdinal() != null) && (t.getOrdinal() < p.getOrdinal())) {
                    output.reportFailure(new AISValidationFailure(lowerOrdinal(p, t)));
                }
            }
        }
    }

    private static AISValidationException noOrdinal(Table t) {
        return new AISValidationException(
            String.format("Table %s has no ordinal", t.getName())
        );
    }

    private static AISValidationException lowerOrdinal(Table parent, Table child) {
        return new AISValidationException(
            String.format("Table %s has ordinal %d lower than parent %s ordinal %d",
                          child.getName(), child.getOrdinal(), parent.getName(), parent.getOrdinal())
        );
    }
}
