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

import java.nio.charset.Charset;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.UnsupportedCharsetException;

/**
 * Verify the table default character set and define character sets for each column
 * are valid and supported. 
 * @author tjoneslo
 *
 */
class CharacterSetSupported implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Table table : ais.getTables().values()) {
            final String tableCharset = table.getDefaultedCharsetName();
            if (tableCharset != null && !Charset.isSupported(tableCharset)) {
                output.reportFailure(new AISValidationFailure (
                        new UnsupportedCharsetException (tableCharset)));
            }
            
            for (Column column : table.getColumnsIncludingInternal()) {
                final String columnCharset = column.getCharsetName();
                if (columnCharset != null && !Charset.isSupported(columnCharset)) {
                    output.reportFailure(new AISValidationFailure (
                            new UnsupportedCharsetException (columnCharset)));
                }
            }
        }
    }
}
