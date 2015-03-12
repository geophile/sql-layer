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
package com.foundationdb.server.types.value;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.DeepCopiable;
import com.foundationdb.server.types.TInstance;

public final class ValueTargets {
    private ValueTargets() {}

    public static void putLong(ValueTarget target, long val)
    {
        switch (underlyingType(target))
        {
            case INT_8:
                target.putInt8((byte)val);
                break;
            case INT_16:
                target.putInt16((short)val);
                break;
            case INT_32:
                target.putInt32((int)val);
                break;
            case INT_64:
                target.putInt64(val);
                break;
            default:
                throw new AkibanInternalException("Cannot put LONG into " + target.getType());
        }
    }
    
    public static UnderlyingType underlyingType(ValueTarget valueTarget){
        return TInstance.underlyingType(valueTarget.getType());
    }

    public static void copyFrom(ValueSource source, ValueTarget target) {
        if (source.isNull()) {
            target.putNull();
            return;
        }
        else if (source.hasCacheValue()) {
            if (target.supportsCachedObjects()) {
                // The BigDecimalWrapper is mutable
                // a shalloow copy won't work.
                Object obj = source.getObject();
                if (obj instanceof DeepCopiable)
                    target.putObject(((DeepCopiable)obj).deepCopy());
                else
                    target.putObject(source.getObject());
                return;
            }
            else if (!source.canGetRawValue()) {
                throw new IllegalStateException("source has only cached object, but no cacher provided: " + source);
            }
        }
        else if (!source.canGetRawValue()) {
            throw new IllegalStateException("source has no value: " + source);
        }
        switch (TInstance.underlyingType(source.getType())) {
        case BOOL:
            target.putBool(source.getBoolean());
            break;
        case INT_8:
            target.putInt8(source.getInt8());
            break;
        case INT_16:
            target.putInt16(source.getInt16());
            break;
        case UINT_16:
            target.putUInt16(source.getUInt16());
            break;
        case INT_32:
            target.putInt32(source.getInt32());
            break;
        case INT_64:
            target.putInt64(source.getInt64());
            break;
        case FLOAT:
            target.putFloat(source.getFloat());
            break;
        case DOUBLE:
            target.putDouble(source.getDouble());
            break;
        case BYTES:
            target.putBytes(source.getBytes());
            break;
        case STRING:
            target.putString(source.getString(), null);
            break;
        default:
            throw new AssertionError(source.getType());
        }
    }


}
