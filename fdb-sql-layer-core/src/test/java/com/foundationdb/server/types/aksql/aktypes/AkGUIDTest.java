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
package com.foundationdb.server.types.aksql.aktypes;

import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

public class AkGUIDTest {

    @Test
    public void UUIDtoUUID() {
        UUID randomUUID = UUID.randomUUID();
        byte[] byteArray = AkGUID.uuidToBytes(randomUUID);
        UUID outputUUID = AkGUID.bytesToUUID(byteArray, 0);
        assertEquals(randomUUID,outputUUID);
    }

    @Test
    public void BytesToBytes() {
        byte byteArray[] = new byte[16];
        for(int i = 0; i < 16; i++){
            byteArray[i] = (byte)i;
        }
        UUID tempUUID = AkGUID.bytesToUUID(byteArray, 0);
        byte[] outputByteArray = AkGUID.uuidToBytes(tempUUID);
        assertArrayEquals(outputByteArray, byteArray);

    }

    @Test
    public void checkUUIDToBytes() {
        String uuidString = "384000008cf011bdb23e10b96e4ef00d";
        UUID uuid = UUID.fromString( "38400000-8cf0-11bd-b23e-10b96e4ef00d");
        byte[] bytes = AkGUID.uuidToBytes(uuid);
        String output = Hex.encodeHexString(bytes);
        assertEquals(output, uuidString);
    }
}
