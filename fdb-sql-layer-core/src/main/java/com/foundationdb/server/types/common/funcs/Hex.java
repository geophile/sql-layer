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
package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.Strings;
import com.google.common.primitives.Ints;

import java.nio.charset.Charset;
import java.util.List;

public class Hex
{
    private Hex() {
    }

    private static final String HEX_NAME = "HEX";

    private static Charset getCharset(TInstance type) {
        return Charset.forName(StringAttribute.charsetName(type));
    }

    public static final TScalar[] create(final TString stringType, final TClass longType, final TBinary binaryType) {

        TScalar hex_string = new TScalarBase()
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(stringType, 0);
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                ValueSource input = inputs.get(0);
                Charset charset = getCharset(input.getType());
                String s = input.getString();
                byte[] bytes = s.getBytes(charset);
                output.putString(Strings.hex(bytes), null);
            }

            @Override
            public String displayName() {
                return HEX_NAME;
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        TInstance type = inputs.get(0).type();
                        int maxLen = type.attribute(StringAttribute.MAX_LENGTH);
                        Charset charset = getCharset(type);
                        long maxBytes = (long)Math.ceil(maxLen * charset.newEncoder().maxBytesPerChar());
                        long maxHexLength = maxBytes * 2;
                        return stringType.instance(Ints.saturatedCast(maxHexLength), anyContaminatingNulls(inputs));
                    }
                });
            }

        };

        TScalar hex_bigint = new TScalarBase()
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(longType, 0);
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                long value = inputs.get(0).getInt64();
                output.putString(Long.toHexString(value).toUpperCase(), null);
            }

            @Override
            public String displayName() {
                return HEX_NAME;
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        // 16 = BIGINT size * 2
                        return stringType.instance(16, anyContaminatingNulls(inputs));
                    }
                });
            }
        };

        TScalar hex_binary = new TScalarBase()
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(binaryType, 0);
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                byte[] bytes = inputs.get(0).getBytes();
                output.putString(Strings.hex(bytes), null);
            }

            @Override
            public String displayName() {
                return HEX_NAME;
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        int length = inputs.get(0).type().attribute(TBinary.Attrs.LENGTH);
                        return stringType.instance(Ints.saturatedCast(length * 2), anyContaminatingNulls(inputs));
                    }
                });
            }
        };

        return new TScalar[] { hex_string, hex_bigint, hex_binary };
    }

}
