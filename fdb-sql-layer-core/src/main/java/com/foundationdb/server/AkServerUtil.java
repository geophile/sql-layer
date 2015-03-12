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
