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
import com.foundationdb.util.ArgumentValidation;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import static org.junit.Assert.*;

public class QuoteTest {
    private static final String TEST_STRING = "world\\ isn't this \" a quote?\u0001";
    @Test
    public void testNoneEncoding() {
        StringBuilder sb = new StringBuilder();
        Quote.NONE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", TEST_STRING, sb.toString());

    }

    @Test
    public void testDoubleEncoding() {
        StringBuilder sb = new StringBuilder();
        Quote.DOUBLE_QUOTE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "world\\\\ isn't this \\\" a quote?\u0001", sb.toString());
    }

    @Test
    public void testSingleEncoding() {
        StringBuilder sb = new StringBuilder();
        Quote.SINGLE_QUOTE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "world\\\\ isn\\'t this \" a quote?\u0001", sb.toString());
    }

    @Test
    public void testJSONEncoding() {
        StringBuilder sb = new StringBuilder();
        Quote.JSON_QUOTE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "world\\\\ isn't this \\\" a quote?\\u0001", sb.toString());
    }
}
