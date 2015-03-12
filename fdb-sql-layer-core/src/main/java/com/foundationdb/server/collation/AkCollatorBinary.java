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
package com.foundationdb.server.collation;

import java.nio.charset.Charset;
import java.util.Arrays;

import com.foundationdb.server.types.common.types.StringFactory;
import com.persistit.Key;

public class AkCollatorBinary extends AkCollator {
    private final Charset UTF8 = Charset.forName("UTF8");

    public AkCollatorBinary() {
        super(AkCollatorFactory.UCS_BINARY, 0);
    }
    
    @Override
    public boolean isRecoverable() {
        return true;
    }

    @Override
    public void append(Key key, String value) {
        key.append(value);
    }

    /**
     * Append the given value to the given key.
     */
    public byte[] encodeSortKeyBytes(String value) {
        return value.getBytes(UTF8);
    }

    /**
     * Recover the value or throw an unsupported exception.
     */
    public String debugDecodeSortKeyBytes(byte[] bytes, int index, int length) {
        return internalDecodeSortKeyBytes(bytes, index, length);
    }

    @Override
    public int compare(String string1, String string2) {
        return string1.compareTo(string2);
    }

    @Override
    public boolean isCaseSensitive() {
        return true;
    }

    @Override
    public String toString() {
        return getScheme();
    }

    @Override
    public int hashCode(String string) {
        return string.hashCode();
    }

    @Override
    public int hashCode(byte[] bytes) {
        return hashCode(internalDecodeSortKeyBytes(bytes, 0, bytes.length));
    }

    private String internalDecodeSortKeyBytes(byte[] bytes, int index, int length) {
        return new String(bytes, index, length, UTF8);
    }
}
