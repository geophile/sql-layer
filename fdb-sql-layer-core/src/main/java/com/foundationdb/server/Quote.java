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

import com.foundationdb.util.AkibanAppender;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;

public enum Quote {
    NONE(null, false),
    SINGLE_QUOTE('\'', false),
    DOUBLE_QUOTE('"', false),
    JSON_QUOTE('"', true);

    private final Character quoteChar;
    private final boolean escapeControlChars;

    Quote(Character quoteChar, boolean escapeControlChars) {
        this.quoteChar = quoteChar;
        this.escapeControlChars = escapeControlChars;
    }

    public void append(AkibanAppender sb, String s) {
        doAppend(sb, s, quoteChar, escapeControlChars);
    }

    private static boolean needsEscaping(char ch) {
        // Anything other than printing ASCII.
        return (ch >= 0200) || Character.isISOControl(ch);
    }

    private static final String SIMPLY_ESCAPED = "\r\n\t";
    private static final String SIMPLY_ESCAPES = "rnt";

    static void doAppend(AkibanAppender sb, String s, Character quote, boolean escapeControlChars) {
        if (s == null) {
            sb.append(null);
            return;
        }
        if (quote == null) {
            if (escapeControlChars) {
                // this is not put in as an assert, so that we can unit test it
                throw new AssertionError("can't escape without quoting, as a simplification to the code");
            }
            sb.append(s);
            return;
        }

        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (escapeControlChars && needsEscaping(ch)) {
                int idx = SIMPLY_ESCAPED.indexOf(ch);
                if (idx < 0) {
                    new Formatter(sb.getAppendable()).format("\\u%04x", (int)ch);
                }
                else {
                    sb.append('\\');
                    sb.append(SIMPLY_ESCAPES.charAt(idx));
                }
            }
            else {
                if (ch == quote || ch == '\\') {
                    sb.append('\\');
                }
                sb.append(ch);
            }
        }
    }
}
