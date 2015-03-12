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

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;


public final class ParameterizationBuilderTest
{
    @Test
    public void testAdd()
    {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.add("hello", 1, "someString", 50L);

        List<Parameterization> params = builder.asList();
        assertEquals("params size", 1, params.size());
        assertEquivalent("only one",
                Parameterization.create("hello", 1, "someString", 50L),
                params.get(0));
    }

    @Test
    public void testAddFailing()
    {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.addFailing("hello", 1, "someString", 50L);

        List<Parameterization> params = builder.asList();
        assertEquals("params size", 1, params.size());
        assertEquivalent("only one",
                Parameterization.failing("hello", 1, "someString", 50L),
                params.get(0));
    }

    @Test
    public void testCreate()
    {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.create("hello", true, 1, "someString", 50L);
        builder.create("hi", false, -1, "anotherString", -50L);

        List<Parameterization> params = builder.asList();
        assertEquals("params size", 2, params.size());
        assertEquivalent("passing",
                new Parameterization("hello", true, new Object[]{1, "someString", 50L}),
                params.get(0));
        assertEquivalent("failing",
                new Parameterization("hi", false, new Object[]{-1, "anotherString", -50L}),
                params.get(1));
    }
    
    @Test
    public void testAppend()
    {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.add("ONE", 1, 2, 3);
        builder.addFailing("TWO", 4, 5, 6);
        builder.multiplyParametersByAppending("-a", 'a', "-b", 'b', "-c", 'c');

        List<Parameterization> expected = Arrays.asList(
                Parameterization.create("ONE-a", 1, 2, 3, 'a'),
                Parameterization.create("ONE-b", 1, 2, 3, 'b'),
                Parameterization.create("ONE-c", 1, 2, 3, 'c'),
                Parameterization.failing("TWO-a", 4, 5, 6, 'a'),
                Parameterization.failing("TWO-b", 4, 5, 6, 'b'),
                Parameterization.failing("TWO-c", 4, 5, 6, 'c')
        );
        List<Parameterization> actual = builder.asList();

        assertEquals("params size", expected.size(), actual.size());

        for(int i=0, len=expected.size(); i<len; ++i)
        {
            assertEquivalent("param " + i, expected.get(i), actual.get(i));
        }
    }

    @Test
    public void testPrepend()
    {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.add("ONE", 1, 2, 3);
        builder.addFailing("TWO", 4, 5, 6);
        builder.multiplyParametersByPrepending("a:", 'a', "b:", 'b', "c:", 'c');

        List<Parameterization> expected = Arrays.asList(
                Parameterization.create("a:ONE", 'a', 1, 2, 3),
                Parameterization.create("b:ONE", 'b', 1, 2, 3),
                Parameterization.create("c:ONE", 'c', 1, 2, 3),
                Parameterization.failing("a:TWO", 'a', 4, 5, 6),
                Parameterization.failing("b:TWO", 'b', 4, 5, 6),
                Parameterization.failing("c:TWO", 'c', 4, 5, 6)
        );
        List<Parameterization> actual = builder.asList();

        assertEquals("params size", expected.size(), actual.size());

        for(int i=0, len=expected.size(); i<len; ++i)
        {
            assertEquivalent("param " + i, expected.get(i), actual.get(i));
        }
    }

    @Test
    public void testListBackedByBuilder()
    {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        List<Parameterization> list = builder.asList();

        assertEquals("list size before building", 0, list.size());

        builder.add("whatever", 'a');
        assertEquals("list size after building", 1, list.size());

        assertSame("new list", builder.asList(), list);
    }

    @Test(expected=IllegalArgumentException.class)
    public void builderMultiplierArgsNotPaired()
    {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.add("whatever", 1);

        builder.multiplyParametersByAppending("label", 2, 3);
    }

    @Test(expected=IllegalArgumentException.class)
    public void builderMultiplierArgsLabelNotString()
    {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.add("whatever", 1);

        builder.multiplyParametersByAppending("label", 2, 3, "this label is in the wrong place");
    }

    @Test(expected=IllegalStateException.class)
    public void builderMultipliesByZero()
    {
        new ParameterizationBuilder().multiplyParametersByAppending();
    }

    private static void assertEquivalent(String message, Parameterization expected, Parameterization actual)
    {
        if (!expected.equivalent(actual))
        {
            fail(message + " expected<" + expected + "> but was <" + actual + ">");
        }
    }
}
