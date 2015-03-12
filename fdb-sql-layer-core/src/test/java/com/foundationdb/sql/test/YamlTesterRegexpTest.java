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
package com.foundationdb.sql.test;

import com.foundationdb.sql.test.YamlTester.Regexp;

import static org.junit.Assert.fail;
import org.junit.Test;

/** Test the YamlTester.Regexp class. */

public class YamlTesterRegexpTest {

    @Test(expected=NullPointerException.class)
    public void testConstructorNull() {
	new Regexp(null);
    }

    @Test
    public void testSimple() {
	test("", "", true);
	test("null", "null", true);
	test("null", null, true);
	test("abc", "abc", true);
	test("abc", "ab", false);
	test("abc", "abcd", false);
	test("1", new Integer(1), true);
	test("1", new Integer(0), false);
	test("1.0", new Double(1.0), true);
	test("1.0", new Double(-1.0), false);
    }

    @Test
    public void testFeatures() {
	test("ab\\\\c", "ab\\c", true);
	test("a.c", "axc", true);
	test("a[bc]d", "abd", true);
	test("a[bc]d", "acd", true);
	test("a[bc]d", "aed", false);
	test("(ab|cd)ef", "abef", true);
	test("(ab|cd)ef", "cdef", true);
	test("(ab|cd)ef", "adef", false);
	test("ab*c", "ac", true);
	test("ab*c", "abc", true);
	test("ab*c", "abbbc", true);
	test("ab*c", "aabc", false);
	test("ab?c", "ac", true);
	test("ab?c", "abc", true);
	test("ab?c", "abbc", false);
	test("ab+c", "ac", false);
	test("ab+c", "abc", true);
	test("ab+c", "abbbbc", true);
	test("ab+c", "abbbbcc", false);
    }

    @Test
    public void testCaptureReference() {
	test("a b c \\{d e f\\}", "a b c {d e f}", true);
	test("([ab]+)([cd]+){1}{2}", "badcbadc", true);
	test("([ab]+)([cd]+){1}{2}", "badcabcd", false);
	test("\\{abc\\}", "{abc}", true);
	test("\\{123\\}", "{123}", true);
    }

    private static void test(String pattern, Object output, boolean match) {
	boolean result = new Regexp(pattern).compareExpected(output);
	if (result != match) {
	    fail("Expected pattern '" + pattern + "' and output '" + output +
		 "' to " + (match ? "" : "not ") + "match");
	}
    }
}
