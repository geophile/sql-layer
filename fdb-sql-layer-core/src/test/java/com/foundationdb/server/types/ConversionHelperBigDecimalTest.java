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
package com.foundationdb.server.types;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public final class ConversionHelperBigDecimalTest {

    @Test
    public void normalizeTruncateNoInt() {
        checkNormalizeToString("1", 4, 4, ".9999");
    }

    @Test
    public void normalizeTruncateNoIntNegative() {
        checkNormalizeToString("-1", 4, 4, "-.9999");
    }

    @Test
    public void normalizeTruncateOnlyInt() {
        checkNormalizeToString("1000000", 4, 0, "9999");
    }

    @Test
    public void normalizeTruncateMixed() {
        checkNormalizeToString("1000000", 4, 2, "99.99");
    }

    @Test
    public void normalizeTruncateFractional() {
        checkNormalizeToString("1.234567", 4, 2, "1.23");
    }

    @Test
    public void normalizeAddPrecision() {
        checkNormalizeToString("2.5", 5, 2, "2.50");
    }

    private void checkNormalizeToString(String in, int precision, int scale, String expected) {
        BigDecimal bigDecimal = new BigDecimal(in);
        String actual = ConversionHelperBigDecimal.normalizeToString(bigDecimal, precision, scale);
        assertEquals(String.format("%s (%d,%d)", in, precision, scale), expected, actual);
    }
}
