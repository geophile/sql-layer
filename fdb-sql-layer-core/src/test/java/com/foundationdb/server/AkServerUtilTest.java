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

import org.junit.Test;

import static org.junit.Assert.*;

public final class AkServerUtilTest {
    private static byte[] byteArray(int... values) {
        byte[] bytes = new byte[values.length];
        for(int i = 0; i < values.length; ++i) {
            bytes[i] = (byte)values[i];
        }
        return bytes;
    }

    @Test
    public void getSignedLong() {
        assertEquals(                    0, AkServerUtil.getLong(byteArray(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00), 0));
        assertEquals(                    1, AkServerUtil.getLong(byteArray(0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00), 0));
        assertEquals( 9223372036854775807L, AkServerUtil.getLong(byteArray(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F), 0));
        assertEquals(-9223372036854775808L, AkServerUtil.getLong(byteArray(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80), 0));
        assertEquals(                   -2, AkServerUtil.getLong(byteArray(0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 0));
        assertEquals(                   -1, AkServerUtil.getLong(byteArray(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 0));
    }
}
