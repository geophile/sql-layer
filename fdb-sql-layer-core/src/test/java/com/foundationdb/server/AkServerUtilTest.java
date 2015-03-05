/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
