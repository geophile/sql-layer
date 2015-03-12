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
package com.foundationdb.junit;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public final class OnlyIfUsageTest {
    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.add("foo", "foo");
        builder.add("bar", "bar");
        return builder.asList();
    }

    private final String string;
    public final boolean stringIsFoo;

    public OnlyIfUsageTest(String string) {
        this.string = string;
        stringIsFoo = "foo".equals(string);
    }

    @Test @OnlyIf("isFoo()")
    public void equalsFoo() {
        assertEquals("string", "foo", string);
    }

    @Test @OnlyIf("stringIsFoo")
    public void equalsFooByField() {
        assertEquals("string", "foo", string);
    }

    @Test @OnlyIfNot("isFoo()")
    public void notEqualsFoo() {
        if ("foo".equals(string)) {
            fail("found a foo!");
        }
    }

    @Test @OnlyIf("hasThreeChars()") @OnlyIfNot("lastCharR()")
    public void threeCharNoTrailingR() {
        assertEquals("string length", 3, string.length());
        assertFalse("last char was r! <" + string + '>', string.charAt(2) == 'r');
    }

    @Test
    public void stringNotNull() {
        assertNotNull("string", string);
    }

    public boolean isFoo() {
        return "foo".equals(string);
    }

    public boolean hasThreeChars() {
        return string.length() == 3;
    }

    public boolean lastCharR() {
        // for simplicity, we'll assume string not null, string.length > 0
        return string.charAt( string.length() - 1) == 'r';
    }
}
