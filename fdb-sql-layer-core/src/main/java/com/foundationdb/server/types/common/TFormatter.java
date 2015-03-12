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
package com.foundationdb.server.types.common;

import com.foundationdb.server.error.LobUnsupportedException;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.Strings;
import java.util.UUID;

public class TFormatter {

    public static enum FORMAT implements TClassFormatter {
        BOOL {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Boolean.toString(source.getBoolean()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(source.getBoolean() ? "TRUE" : "FALSE");
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                format(type, source, out);
            }
        },
        GUID {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(((UUID) source.getObject()).toString());
            }
    
            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                out.append("'");
                format(type, source, out);
                out.append("'");
            }
    
            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                out.append("\"");
                format(type, source, out);
                out.append("\"");
            }
        },
        BLOB {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                throw new LobUnsupportedException("Formatting BLOB as string is unsupported");
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                BlobRef blob = (BlobRef) source.getObject();
                if ( blob.isReturnedBlobInUnwrappedMode() || blob.isShortLob()) {
                    byte[] value = source.getBytes();
                    out.append("X'");
                    out.append(Strings.hex(value));
                    out.append("'");
                }
                else if (blob.isLongLob()) {
                    out.append("'");
                    format(type, source, out);
                    out.append("'");
                }
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                BlobRef blob = (BlobRef) source.getObject();
                if (blob.isReturnedBlobInUnwrappedMode() || blob.isShortLob()) {
                    String formattedString = options.get(FormatOptions.JsonBinaryFormatOption.class).format(blob.getBytes());
                    out.append("\"" + formattedString + "\"");
                }
                else if (blob.isLongLob()) {
                    out.append("\"" + blob.getId().toString() + "\"");
                }
            }
        };
    }
    
    
}
