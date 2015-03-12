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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.Strings;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.List;

public class Base64
{
    private Base64() {
    }

    public static final TScalar[] create(final TString varchar, final TBinary varbinary) {

        TScalar binary_to_base64 = new TScalarBase()
        {
            @Override
            public String displayName() {
                return "TO_BASE64";
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(varbinary, 0);
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        TInstance inputType = inputs.get(0).type();
                        int binaryLength = inputType.attribute(TBinary.Attrs.LENGTH);
                        int base64Length = (binaryLength * 4 + 2) / 3; // round up for ='s
                        return varchar.instance(base64Length, inputType.nullability());
                    }        
                });
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                byte[] binary = inputs.get(0).getBytes();
                output.putString(Strings.toBase64(binary), null);
            }
        };

        TScalar string_to_base64 = new TScalarBase()
        {
            @Override
            public String displayName() {
                return "TO_BASE64";
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(varchar, 0);
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        TInstance inputType = inputs.get(0).type();
                        int stringLength = inputType.attribute(StringAttribute.MAX_LENGTH);
                        int encodedLength = (int)Math.ceil(stringLength * Charset.forName(StringFactory.Charset.of(inputType.attribute(StringAttribute.CHARSET))).newEncoder().maxBytesPerChar());
                        int base64Length = (encodedLength * 4 + 2) / 3; // round up for ='s
                        return varchar.instance(base64Length, inputType.nullability());
                    }        
                });
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                ValueSource input = inputs.get(0);
                String charset = StringFactory.Charset.of(input.getType().attribute(StringAttribute.CHARSET));
                String string = input.getString();
                try {
                    byte[] binary = string.getBytes(charset);
                    output.putString(Strings.toBase64(binary), null);
                }
                catch (UnsupportedEncodingException ex)
                {
                    context.warnClient(new InvalidParameterValueException("Unknown CHARSET: " + charset));
                    output.putNull();
                }
            }
        };

        TScalar from_base64 = new TScalarBase()
        {
            @Override
            public String displayName() {
                return "FROM_BASE64";
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(varchar, 0);
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        TInstance inputType = inputs.get(0).type();
                        int stringLength = inputType.attribute(StringAttribute.MAX_LENGTH);
                        int binaryLength = stringLength / 4 * 3;
                        return varbinary.instance(binaryLength, inputType.nullability());
                    }        
                });
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                String base64 = inputs.get(0).getString();
                output.putBytes(Strings.fromBase64(base64));
            }
        };

        return new TScalar[] { binary_to_base64, string_to_base64, from_base64 };
    }

}
