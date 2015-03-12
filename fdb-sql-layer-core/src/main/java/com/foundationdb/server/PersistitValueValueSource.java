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
package com.foundationdb.server;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

import java.util.HashMap;
import java.util.Map;

public final class PersistitValueValueSource implements ValueSource {

    // PersistitValueValueSource interface
    
    public void attach(com.persistit.Value value)
    {
        this.persistitValue = value;
        clear();
        value.setStreamMode(true);
    }

    public void getReady(TInstance expectedTInstance) {
        if (persistitValue.isNull(true)) {
            cacheObject = NULL;
        }
        else {
            Class<?> valueClass = persistitValue.getType();
            UnderlyingType rawUnderlying = classesToUnderlying.get(valueClass);
            if (rawUnderlying == TInstance.underlyingType(expectedTInstance)) {
                value.underlying(expectedTInstance);
                cacheObject = null;
            }
            else
                cacheObject = READY_FOR_CACHE;
        }
    }
    
    // ValueSource interface

    @Override
    public boolean hasAnyValue() {
        return (persistitValue != null);
    }

    @Override
    public boolean hasRawValue() {
        return hasAnyValue() && (cacheObject == null);
    }

    @Override
    public boolean hasCacheValue() {
        return (cacheObject != null) && (cacheObject != NULL);
    }

    @Override
    public boolean canGetRawValue() {
        return hasRawValue();
    }

    private boolean needsDecoding(UnderlyingType toUnderlying) {
        assert toUnderlying == TInstance.underlyingType(value.getType())
                : "expected underlying " + toUnderlying + " but was set up for " + value.getType();
        return ! value.hasRawValue();
    }
    
    @Override
    public Object getObject() {
        if (cacheObject == null)
            throw new IllegalStateException("no cache object: " + value);
        if (cacheObject == READY_FOR_CACHE)
            cacheObject = persistitValue.get();
        return (cacheObject == NULL) ? null : cacheObject;
    }

    @Override
    public TInstance getType() {
        assert hasRawValue() : "underlying type is only available when there is a raw value";
        return value.getType();
    }

    @Override
    public boolean isNull() {
        return cacheObject == NULL;
    }

    @Override
    public boolean getBoolean() {
        if (needsDecoding(UnderlyingType.BOOL))
            value.putBool(persistitValue.getBoolean());
        return value.getBoolean();
    }

    @Override
    public boolean getBoolean(boolean defaultValue) {
        return isNull() ? defaultValue : getBoolean();
    }

    @Override
    public byte getInt8() {
        if (needsDecoding(UnderlyingType.INT_8))
            value.putInt8(persistitValue.getByte());
        return value.getInt8();
    }

    @Override
    public short getInt16() {
        if (needsDecoding(UnderlyingType.INT_16))
            value.putInt16(persistitValue.getShort());
        return value.getInt16();
    }

    @Override
    public char getUInt16() {
        if (needsDecoding(UnderlyingType.UINT_16))
            value.putUInt16(persistitValue.getChar());
        return value.getUInt16();
    }

    @Override
    public int getInt32() {
        if (needsDecoding(UnderlyingType.INT_32))
            value.putInt32(persistitValue.getInt());
        return value.getInt32();
    }

    @Override
    public long getInt64() {
        if (needsDecoding(UnderlyingType.INT_64))
            value.putInt64(persistitValue.getLong());
        return value.getInt64();
    }

    @Override
    public float getFloat() {
        if (needsDecoding(UnderlyingType.FLOAT))
            value.putFloat(persistitValue.getFloat());
        return value.getFloat();
    }

    @Override
    public double getDouble() {
        if (needsDecoding(UnderlyingType.DOUBLE))
            value.putDouble(persistitValue.getDouble());
        return value.getDouble();
    }

    @Override
    public byte[] getBytes() {
        if (needsDecoding(UnderlyingType.BYTES))
            value.putBytes(persistitValue.getByteArray());
        return value.getBytes();
    }

    @Override
    public String getString() {
        if (needsDecoding(UnderlyingType.STRING))
            value.putString(persistitValue.getString(), null);
        return value.getString();
    }

    // private
    
    private void clear() {
        value.underlying(null);
    }
    
    // object state


    public PersistitValueValueSource() {
        clear();
    }

    private com.persistit.Value persistitValue;
    private Value value = new Value();
    private Object cacheObject = null;
    
    private static final Object READY_FOR_CACHE = new Object();
    private static final Object NULL = new Object();
    private static final Map<Class<?>,UnderlyingType> classesToUnderlying = createTranslationMap();

    private static Map<Class<?>, UnderlyingType> createTranslationMap() {
        Map<Class<?>,UnderlyingType> map = new HashMap<>(UnderlyingType.values().length);
        map.put(boolean.class, UnderlyingType.BOOL);
        map.put(byte.class, UnderlyingType.INT_8);
        map.put(short.class, UnderlyingType.INT_16);
        map.put(char.class, UnderlyingType.UINT_16);
        map.put(int.class, UnderlyingType.INT_32);
        map.put(long.class, UnderlyingType.INT_64);
        map.put(float.class, UnderlyingType.FLOAT);
        map.put(double.class, UnderlyingType.DOUBLE);
        map.put(byte[].class, UnderlyingType.BYTES);
        map.put(String.class, UnderlyingType.STRING);
        return map;
    }
}
