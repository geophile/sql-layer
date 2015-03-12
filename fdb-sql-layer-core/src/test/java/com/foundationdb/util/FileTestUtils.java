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
package com.foundationdb.util;

public class FileTestUtils {

    private static final String SOURCE_PREFIX = "src/test/resources/";
    private static final String TARGET_PREFIX = "target/test-classes/";

    public static void printClickableFile(String filename, String suffix, int lineNumber) {
        if (filename != null) {
            String relative = filename;
            int idx = relative.indexOf(SOURCE_PREFIX);
            if (idx >= 0) {
                relative = relative.substring(idx + SOURCE_PREFIX.length());
            }
            else {
                idx = relative.indexOf(TARGET_PREFIX);
                if (idx >= 0) {
                    relative = relative.substring(idx + TARGET_PREFIX.length());
                }
            }
            System.err.println("  at " + relative.replaceFirst("/([^/]+.)$", "($1." + suffix + ":" + lineNumber + ")").replaceAll("/", "."));
            // for those running from maven or elsewhere
            System.err.println("  aka: " + filename + "." + suffix + ":" + lineNumber);
        } else {
            System.err.println("NULL filename");
        }
    }
}
