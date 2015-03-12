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

import org.junit.Assert;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class AssertUtils {
    public static void assertCollectionEquals(String message, Collection<?> expected, Collection<?> actual) {
        String expectedString = Strings.join(collectionToString(expected));
        String actualString = Strings.join(collectionToString(actual));
        Assert.assertEquals(message, expectedString, actualString);
        Assert.assertEquals(message, expected, actual);
    }

    public static void assertMapEquals(String message, Map<?,?> expected, Map<?,?> actual) {
        String expectedString = Strings.join(Strings.stringAndSort(expected));
        String actualString = Strings.join(Strings.stringAndSort(actual));
        Assert.assertEquals(message, expectedString, actualString);
        Assert.assertEquals(message, expected, actual);
    }

    public static void assertCollectionEquals(Collection<?> expected, Collection<?> actual) {
        assertCollectionEquals(null, expected, actual);
    }

    private static List<String> collectionToString(Collection<?> expected) {
        if (expected instanceof List)
            return Strings.mapToString(expected);
        else
            return Strings.stringAndSort(expected);
    }
}
