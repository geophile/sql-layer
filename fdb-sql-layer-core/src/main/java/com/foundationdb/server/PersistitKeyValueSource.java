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

import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.persistit.Key;
import com.foundationdb.server.types.value.Value;

import java.util.EnumMap;

import static java.lang.Math.min;

public class PersistitKeyValueSource implements ValueSource {

    // object state
    private Key key;
    private int depth;
    private TInstance type;
    private Value output;
    private boolean needsDecoding = true;
    
    public PersistitKeyValueSource(TInstance type) {
        this.type = type;
        this.output = new Value(type);
    }
    
    public void attach(Key key, IndexColumn indexColumn) {
        attach(key, indexColumn.getPosition(), indexColumn.getColumn().getType());
    }

    public void attach(Key key, int depth, TInstance type) {
        this.key = key;
        this.depth = depth;
        this.type = type;
        clear();
    }
    
    public void attach(Key key) {
        this.key = key;
        clear();
    }

    @Override
    public TInstance getType() {
        return type;
    }

    @Override
    public boolean hasAnyValue() {
        return decode().hasAnyValue();
    }

    @Override
    public boolean hasRawValue() {
        return decode().hasRawValue();
    }

    @Override
    public boolean hasCacheValue() {
        return decode().hasCacheValue();
    }

    @Override
    public boolean canGetRawValue() {
        return decode().canGetRawValue();
    }

    @Override
    public boolean isNull() {
        /*
         * No need to decode the value to detect null
         */
        if (needsDecoding) {
            key.indexTo(depth);
            return key.isNull();
        }
        return decode().isNull();
    }

    @Override
    public boolean getBoolean() {
        return decode().getBoolean();
    }

    @Override
    public boolean getBoolean(boolean defaultValue) {
        return decode().getBoolean(defaultValue);
    }

    @Override
    public byte getInt8() {
        return decode().getInt8();
    }

    @Override
    public short getInt16() {
        return decode().getInt16();
    }

    @Override
    public char getUInt16() {
        return decode().getUInt16();
    }

    @Override
    public int getInt32() {
        return decode().getInt32();
    }

    @Override
    public long getInt64() {
        return decode().getInt64();
    }

    @Override
    public float getFloat() {
        return decode().getFloat();
    }

    @Override
    public double getDouble() {
        return decode().getDouble();
    }

    @Override
    public byte[] getBytes() {
        return decode().getBytes();
    }

    @Override
    public String getString() {
        return decode().getString();
    }

    @Override
    public Object getObject() {
        return decode().getObject();
    }
    
    public int compare(PersistitKeyValueSource that)
    {
        that.key.indexTo(that.depth);
        int thatPosition = that.key.getIndex();
        that.key.indexTo(that.depth + 1);
        int thatEnd = that.key.getIndex();
        return compareOneKeySegment(that.key.getEncodedBytes(), thatPosition, thatEnd);
    }

    public int compare(byte[] bytes)
    {
        Key thatKey = new Key(key);
        thatKey.clear();
        thatKey.append(bytes);
        thatKey.indexTo(0);
        int thatPosition = thatKey.getIndex();
        thatKey.indexTo(1);
        int thatEnd = thatKey.getIndex();
        return compareOneKeySegment(thatKey.getEncodedBytes(), thatPosition, thatEnd);
    }

    // for use by this class
    private ValueSource decode() {
        if (needsDecoding) {
            key.indexTo(depth);
            if (key.isNull()) {
                output.putNull();
            }
            else
            {
                UnderlyingType underlyingType = TInstance.underlyingType(getType());
                Class<?> expected = underlyingExpectedClasses.get(underlyingType);
                if (key.decodeType() == expected) {
                    switch (underlyingType) {
                        case BOOL:      output.putBool(key.decodeBoolean());        break;
                        case INT_8:     output.putInt8((byte)key.decodeLong());     break;
                        case INT_16:    output.putInt16((short)key.decodeLong());   break;
                        case UINT_16:   output.putUInt16((char)key.decodeLong());   break;
                        case INT_32:    output.putInt32((int)key.decodeLong());     break;
                        case INT_64:    output.putInt64(key.decodeLong());          break;
                        case FLOAT:     output.putFloat(key.decodeFloat());         break;
                        case DOUBLE:    output.putDouble(key.decodeDouble());       break;
                        case BYTES:     output.putBytes(key.decodeByteArray());     break;
                        case STRING:    output.putString(key.decodeString(), null); break;
                        default: throw new UnsupportedOperationException(type + " with " + underlyingType);
                    }
                }
                else {
                    output.putObject(key.decode());
                }
                // the following asumes that the TClass' readCollating expects the same UnderlyingType for in and out
                type.readCollating(output, output);
            }
            needsDecoding = false;
        }
        return output;
    }
    
    private int compareOneKeySegment(byte[] thatBytes, int thatPosition, int thatEnd)
    {
        this.key.indexTo(this.depth);
        int thisPosition = this.key.getIndex();
        this.key.indexTo(this.depth + 1);
        int thisEnd = this.key.getIndex();
        byte[] thisBytes = this.key.getEncodedBytes();
        // Compare until end or mismatch
        int thisN = thisEnd - thisPosition;
        int thatN = thatEnd - thatPosition;
        int n = min(thisN, thatN);
        int end = thisPosition + n;
        while (thisPosition < end) {
            int c = thisBytes[thisPosition++] - thatBytes[thatPosition++];
            if (c != 0) {
                return c;
            }
        }
        return thisN - thatN;
    }

    private void clear() {
        needsDecoding = true;
    }

    private static final EnumMap<UnderlyingType, Class<?>> underlyingExpectedClasses = createPUnderlyingExpectedClasses();

    private static EnumMap<UnderlyingType, Class<?>> createPUnderlyingExpectedClasses() {
        EnumMap<UnderlyingType, Class<?>> result = new EnumMap<>(UnderlyingType.class);
        for (UnderlyingType underlyingType : UnderlyingType.values()) {
            final Class<?> expected;
            switch (underlyingType) {
            case BOOL:
                expected = Boolean.class;
                break;
            case INT_8:
            case INT_16:
            case UINT_16:
            case INT_32:
            case INT_64:
                expected = Long.class;
                break;
            case FLOAT:
                expected = Float.class;
                break;
            case DOUBLE:
                expected = Double.class;
                break;
            case BYTES:
                expected = byte[].class;
                break;
            case STRING:
                expected = String.class;
                break;
            default:
                throw new AssertionError("unrecognized UnderlyingType: " + underlyingType);
            }
            result.put(underlyingType, expected);
        }
        return result;
    }
}
