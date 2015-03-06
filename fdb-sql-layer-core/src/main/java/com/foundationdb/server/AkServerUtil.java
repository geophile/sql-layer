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

import java.io.File;
import java.math.BigInteger;

public class AkServerUtil {

    public static long getLong(byte[] bytes, int index) {
        return (bytes[index] & 0xFFL)
                | (bytes[index + 1] & 0xFFL) << 8
                | (bytes[index + 2] & 0xFFL) << 16
                | (bytes[index + 3] & 0xFFL) << 24
                | (bytes[index + 4] & 0xFFL) << 32
                | (bytes[index + 5] & 0xFFL) << 40
                | (bytes[index + 6] & 0xFFL) << 48
                | (bytes[index + 7] & 0xFFL) << 56;
    }

    public static void putLong(byte[] bytes, int index, long value) {
        bytes[index]     = (byte) (value);
        bytes[index + 1] = (byte) (value >>> 8);
        bytes[index + 2] = (byte) (value >>> 16);
        bytes[index + 3] = (byte) (value >>> 24);
        bytes[index + 4] = (byte) (value >>> 32);
        bytes[index + 5] = (byte) (value >>> 40);
        bytes[index + 6] = (byte) (value >>> 48);
        bytes[index + 7] = (byte) (value >>> 56);
    }

    public final static boolean cleanUpDirectory(final File file) {
        if (!file.exists()) {
            return file.mkdirs();
        } else if (file.isFile()) {
            return false;
        } else {
            boolean success = true;
            final File[] files = file.listFiles();
            if (files != null) {
                if (!cleanUpFiles(files)) {
                    success = false;
                }
            }
            return success;
        }
    }

    public final static boolean cleanUpFiles(final File[] files) {
        boolean success = true;
        for (final File file : files) {
            boolean success1 = true;
            if (file.isDirectory()) {
                success1 = cleanUpDirectory(file);
            }
            if (success1) {
                success1 = file.delete();
            }
            if (!success1) {
                file.deleteOnExit();
                success = false;
            }
        }
        return success;
    }
}
