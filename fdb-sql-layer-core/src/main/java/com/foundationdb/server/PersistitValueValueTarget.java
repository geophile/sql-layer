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

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueTarget;
import com.persistit.Value;

public final class PersistitValueValueTarget implements ValueTarget {
    
    // PersistitValueValueTarget interface
    
    public void attach(Value value) {
        this.value = value;
    }
    
    // ValueTarget interface
    
    @Override
    public boolean supportsCachedObjects() {
        return false;
    }

    @Override
    public void putObject(Object object) {
        value.put(object);
    }

    @Override
    public TInstance getType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNull() {
        value.putNull();
    }

    @Override
    public void putBool(boolean value) {
        this.value.put(value);
    }

    @Override
    public void putInt8(byte value) {
        this.value.put(value);
    }

    @Override
    public void putInt16(short value) {
        this.value.put(value);
    }

    @Override
    public void putUInt16(char value) {
        this.value.put(value);
    }

    @Override
    public void putInt32(int value) {
        this.value.put(value);
    }

    @Override
    public void putInt64(long value) {
        this.value.put(value);
    }

    @Override
    public void putFloat(float value) {
        this.value.put(value);
    }

    @Override
    public void putDouble(double value) {
        this.value.put(value);
    }

    @Override
    public void putBytes(byte[] value) {
        this.value.put(value);
    }

    @Override
    public void putString(String value, AkCollator collator) {
        this.value.put(value);
    }
    
    private Value value;
}
