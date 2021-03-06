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
package com.foundationdb.server.types;

import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.Strings;

public abstract class TClassBase extends TClass
{
    private final TParser parser;
    private final int defaultVarcharLen;
    
    protected <A extends Enum<A> & Attribute> TClassBase(TBundleID bundle,
            String name,
            Enum<?> category,
            Class<A> enumClass,
            TClassFormatter formatter,
            int internalRepVersion, int sVersion, int sSize,
            UnderlyingType underlyingType,
            TParser parser,
            int defaultVarcharLen)
     {
         super(bundle,
               name,
               category,
               enumClass,
               formatter,
               internalRepVersion,
               sVersion,
               sSize,
                 underlyingType);
         
         this.parser = parser;
         this.defaultVarcharLen = defaultVarcharLen;
     }

    @Override
    public void fromObject(TExecutionContext context, ValueSource in, ValueTarget out) {
        if (in.isNull())
            out.putNull();
        else if (!tryFromObject(context, in, out))
            parser.parse(context, in, out);
    }

    @Override
    public TCast castToVarchar() {
        return new TCastBase(this, MString.VARCHAR) {
            @Override
            protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
                AkibanAppender appender = (AkibanAppender) context.exectimeObjectAt(APPENDER_CACHE_INDEX);
                StringBuilder sb;
                if (appender == null) {
                    sb = new StringBuilder();
                    appender = AkibanAppender.of(sb);
                    context.putExectimeObject(APPENDER_CACHE_INDEX, appender);
                }
                else {
                    sb = (StringBuilder) appender.getAppendable();
                    sb.setLength(0);
                }
                format(source.getType(), source, appender);
                String string = sb.toString();
                int maxlen = context.outputType().attribute(StringAttribute.MAX_LENGTH);
                String trunc = Strings.truncateIfNecessary(string, maxlen);
                if (string != trunc) {
                    context.reportTruncate(string, trunc);
                    string = trunc;
                }
                target.putString(string, null);
            }

            @Override
            public TInstance preferredTarget(TPreptimeValue source) {
                int len;
                if (source.value() == null) {
                    len = defaultVarcharLen;
                }
                else {
                    StringBuilder sb = new StringBuilder();
                    format(source.type(), source.value(), AkibanAppender.of(sb));
                    len = sb.length();
                }
                return MString.VARCHAR.instance(len, source.isNullable());
            }
        };
    }

    @Override
    public TCast castFromVarchar() {
        return new TCastBase(MString.VARCHAR, this) {
            @Override
            protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
                parser.parse(context, source, target);
            }
        };
    }

    protected boolean tryFromObject(TExecutionContext context, ValueSource in, ValueTarget out) {
        if (in.getType().equalsExcludingNullable(out.getType())) {
            ValueTargets.copyFrom(in, out);
            return true;
        }
        
        
        UnderlyingType underlyingType = TInstance.underlyingType(in.getType());
        if (underlyingType == UnderlyingType.STRING || underlyingType == UnderlyingType.BYTES)
            return false;
        final String asString;
        switch (underlyingType) {
        case BOOL:
            asString = Boolean.toString(in.getBoolean());
            break;
        case INT_8:
            asString = Byte.toString(in.getInt8());
            break;
        case INT_16:
            asString = Short.toString(in.getInt16());
            break;
        case UINT_16:
            asString = Integer.toString(in.getUInt16());
            break;
        case INT_32:
            asString = Integer.toString(in.getInt32());
            break;
        case INT_64:
            asString = Long.toString(in.getInt64());
            break;
        case FLOAT:
            asString = Float.toString(in.getFloat());
            break;
        case DOUBLE:
            asString = Double.toString(in.getDouble());
            break;
        case BYTES:
        case STRING:
        default:
            throw new AssertionError(underlyingType + ": " + in);
        }
        parser.parse(context, new Value(MString.varcharFor(asString), asString), out);
        return true;
    }

    private static final int APPENDER_CACHE_INDEX = 0;
}
