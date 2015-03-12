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
package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.google.common.collect.ObjectArrays;

import java.io.UnsupportedEncodingException;

/**
 *
 * Implement the length (char_length and octet_length)
 */
public abstract class MLength extends TScalarBase
{
    public static final TScalar CHAR_LENGTH = new MLength("CHAR_LENGTH")
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
        {
            String str = inputs.get(0).getString();
            output.putInt32(str.codePointCount(0, str.length()));
        }

        @Override
        public String[] registeredNames() {
            return new String[] { "char_length", "charLength" };
        }
    };

    public static final TScalar OCTET_LENGTH = new MBinaryLength("OCTET_LENGTH", 1, "getOctetLength");
    public static final TScalar BIT_LENGTH = new MBinaryLength("BIT_LENGTH", 8);

    private static class MBinaryLength extends MLength
    {

        private final int multiplier;
        private final String[] aliases;

        private MBinaryLength(String name, int multiplier, String... aliases) {
            super(name);
            this.multiplier = multiplier;
            this.aliases = ObjectArrays.concat(aliases, name);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
        {
            ValueSource input = inputs.get(0);
            int charsetId = input.getType().attribute(StringAttribute.CHARSET);
            String charset = (StringFactory.Charset.values())[charsetId].name();
            try
            {
                int length = (input.getString()).getBytes(charset).length;
                length *= multiplier;
                output.putInt32(length);
            }
            catch (UnsupportedEncodingException ex) // impossible to happen
            {
                context.warnClient(new InvalidParameterValueException("Unknown CHARSET: " + charset));
                output.putNull();
            }
        }

        @Override
        public String[] registeredNames() {
            return aliases;
        }
    }
    
    private final String name;

    private MLength (String name)
    {
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MString.VARCHAR, 0);
    }



    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT, 10);
    }
}
