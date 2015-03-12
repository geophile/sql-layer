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

import com.foundationdb.server.types.ConversionHelperBigDecimal;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.Strings;
import com.google.common.primitives.UnsignedLongs;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Formatter;
import java.util.Locale;

public class NumericFormatter {

    public static enum FORMAT implements TClassFormatter {
        FLOAT {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Float.toString(source.getFloat()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                new Formatter(out.getAppendable(), Locale.US).format("%e", source.getFloat());
            }
        },
        DOUBLE {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Double.toString(source.getDouble()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                new Formatter(out.getAppendable(), Locale.US).format("%e", source.getDouble());
            }
        },
        INT_8 {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Byte.toString(source.getInt8()));
            }
        },
        INT_16 {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Short.toString(source.getInt16()));
            }
        },
        INT_32 {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Integer.toString(source.getInt32()));
            }
        },
        INT_64 {
            
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Long.toString(source.getInt64()));
            }
        },
        UINT_64 {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(UnsignedLongs.toString(source.getInt64()));
            }
        },
        BYTES {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                String charsetName = StringFactory.DEFAULT_CHARSET.name();
                Charset charset = Charset.forName(charsetName);
                String str = new String(source.getBytes(), charset);
                out.append(str);
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                byte[] value = source.getBytes();
                out.append("X'");
                out.append(Strings.hex(value));
                out.append("'");
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                // There is no strong precedent for how to encode
                // arbitrary bytes in JSON.
                byte[] bytes = source.getBytes();
                String formattedString = options.get(FormatOptions.JsonBinaryFormatOption.class).format(bytes);
                out.append("\"" + formattedString + "\"");
            }
        },
        BIGDECIMAL{
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                if (source.hasCacheValue()) {
                    BigDecimal num = ((BigDecimalWrapper) source.getObject()).asBigDecimal();
                    // toString() uses exponent notation, which SQL reserves for appoximate literals
                    out.append(num.toPlainString());
                }
                else {
                    int precision = type.attribute(DecimalAttribute.PRECISION);
                    int scale = type.attribute(DecimalAttribute.SCALE);
                    ConversionHelperBigDecimal.decodeToString(source.getBytes(), 0, precision, scale, out);
                }
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                // The JSON spec just has one kind of number, so we could output with
                // quotes and reserve scientific notation for floats. But almost every
                // library interprets decimal point as floating point,
                // so stick with string.
                out.append('"');
                format(type, source, out);
                out.append('"');
            }
        };

        @Override
        public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
            format(type, source, out);
        }
        
        @Override
        public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
            format(type, source, out);
        }
    }
}
