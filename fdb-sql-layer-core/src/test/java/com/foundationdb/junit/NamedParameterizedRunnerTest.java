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

import com.foundationdb.junit.NamedParameterizedRunner.ReifiedParamRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.Runner;
import org.junit.runners.model.FrameworkMethod;

import java.util.*;

import static org.junit.Assert.*;

public final class NamedParameterizedRunnerTest
{
    private final static List<Parameterization> builderList = new LinkedList<>();
    private Properties oldProperties;
    private Properties workingProperties;

    @Before
    public void setUp()
    {
        builderList.clear();
        oldProperties = System.getProperties();
        workingProperties = new Properties(oldProperties);
        System.setProperties(workingProperties);
    }

    @After
    public void tearDown()
    {
        System.setProperties(oldProperties);
    }

    public final static class RunEverythingTestExample
    {
        @NamedParameterizedRunner.TestParameters
        @SuppressWarnings("unused")
        public static List<Parameterization> params() { return builderList; }

        public RunEverythingTestExample(char unused)
        {
            assert unused != 'a'; // just to shut it up about it not being used
        }

        @Test
        public void one() {}

        @Test
        public void two() {}
    }

    public final static class OneFailingTestExample
    {
        @NamedParameterizedRunner.TestParameters
        @SuppressWarnings("unused")
        public static List<Parameterization> params() { return builderList; }

        public OneFailingTestExample(char unused)
        {
            assert unused != 'a'; // just to shut it up about it not being used
        }

        @Test
        public void passing() {}

        @Failing @Test
        public void failing() {}
    }

    public final static class OnlyIfTestExample
    {
        @NamedParameterizedRunner.TestParameters
        @SuppressWarnings("unused")
        public static List<Parameterization> params() { return builderList; }

        private final int number;
        @SuppressWarnings("unused") public final boolean numberIs1; // used by @OnlyIf

        public OnlyIfTestExample(int number) {
            this.number = number;
            this.numberIs1 = is1();
        }

        @Test
        public void alwaysRun() {}

        @Test @OnlyIf("is1()")
        public void testIs1Method() {
            assertEquals("number", 1, number);
        }

        @Test @OnlyIf("numberIs1")
        public void testIs1Field() {
            assertEquals("number", 1, number);
        }

        public boolean is1() {
            return number == 1;
        }
    }

    @Test
    public void testRegexUsed()
    {
        assertFalse("//", NamedParameterizedRunner.paramNameUsesRegex("//"));
        assertTrue("/a/", NamedParameterizedRunner.paramNameUsesRegex("/a/"));
    }

    @Test
    public void testRegexMatches()
    {
        assertFalse("/a/ against 'foo'", NamedParameterizedRunner.paramNameMatchesRegex("foo", "/a/"));
        assertTrue("/a/ against 'foo bar'", NamedParameterizedRunner.paramNameMatchesRegex("foo bar", "/a/"));
        assertFalse("/foo/ against 'o'", NamedParameterizedRunner.paramNameMatchesRegex("o", "/foo/"));
    }

    @Test
    public void testRunEverythingPasses() throws Throwable
    {
        ParameterizationBuilder builder = new ParameterizationBuilder(builderList);
        builder.add("alpha", 'a');
        builder.add("beta", 'b');

        NamedParameterizedRunner runner = new NamedParameterizedRunner(RunEverythingTestExample.class);

        Map<String,ReifiedParamRunner> map = testParameterizations(runner,
                "Parameterization[PASSING alpha: [a] ]",
                "Parameterization[PASSING beta: [b] ]");

        testOverrides(map.values(), false);

        testFrameworkMethod(map.get("Parameterization[PASSING alpha: [a] ]"), RunEverythingTestExample.class, "one", true);
        testFrameworkMethod(map.get("Parameterization[PASSING alpha: [a] ]"), RunEverythingTestExample.class, "two", true);
        testFrameworkMethod(map.get("Parameterization[PASSING beta: [b] ]"), RunEverythingTestExample.class, "one", true);
        testFrameworkMethod(map.get("Parameterization[PASSING beta: [b] ]"), RunEverythingTestExample.class, "two", true);
    }

    @Test
    public void testRunEverythingAlphaPasses() throws Throwable
    {
        ParameterizationBuilder builder = new ParameterizationBuilder(builderList);
        builder.add("alpha", 'a');
        builder.addFailing("beta", 'b');

        NamedParameterizedRunner runner = new NamedParameterizedRunner(RunEverythingTestExample.class);

        Map<String,ReifiedParamRunner> map = testParameterizations(runner,
                "Parameterization[PASSING alpha: [a] ]",
                "Parameterization[FAILING beta: [b] ]");

        testOverrides(map.values(), false);

        testFrameworkMethod(map.get("Parameterization[PASSING alpha: [a] ]"), RunEverythingTestExample.class, "one", true);
        testFrameworkMethod(map.get("Parameterization[PASSING alpha: [a] ]"), RunEverythingTestExample.class, "two", true);
        testFrameworkMethod(map.get("Parameterization[FAILING beta: [b] ]"), RunEverythingTestExample.class, "one", false);
        testFrameworkMethod(map.get("Parameterization[FAILING beta: [b] ]"), RunEverythingTestExample.class, "two", false);
    }

    @Test
    public void testOneMethodFailing() throws Throwable
    {
        ParameterizationBuilder builder = new ParameterizationBuilder(builderList);
        builder.add("alpha", 'a');
        builder.add("beta", 'b');

        NamedParameterizedRunner runner = new NamedParameterizedRunner(OneFailingTestExample.class);

        Map<String,ReifiedParamRunner> map = testParameterizations(runner,
                "Parameterization[PASSING alpha: [a] ]",
                "Parameterization[PASSING beta: [b] ]");

        testOverrides(map.values(), false);

        testFrameworkMethod(map.get("Parameterization[PASSING alpha: [a] ]"), OneFailingTestExample.class, "passing", true);
        testFrameworkMethod(map.get("Parameterization[PASSING alpha: [a] ]"), OneFailingTestExample.class, "failing", false);
        testFrameworkMethod(map.get("Parameterization[PASSING beta: [b] ]"), OneFailingTestExample.class, "passing", true);
        testFrameworkMethod(map.get("Parameterization[PASSING beta: [b] ]"), OneFailingTestExample.class, "failing", false);
    }

    @Test
    public void testOverrideOfFailingMethod() throws Throwable
    {
        workingProperties.put(NamedParameterizedRunner.PARAMETERIZATION_OVERRIDE, "beta");
        ParameterizationBuilder builder = new ParameterizationBuilder(builderList);
        builder.add("alpha", 'a');
        builder.add("beta", 'b');

        NamedParameterizedRunner runner = new NamedParameterizedRunner(OneFailingTestExample.class);

        Map<String,ReifiedParamRunner> map = testParameterizations(runner,
                "Parameterization[PASSING beta: [b] ]");

        testOverrides(map.values(), true);
        
        testFrameworkMethod(map.get("Parameterization[PASSING beta: [b] ]"), OneFailingTestExample.class, "passing", true);
        testFrameworkMethod(map.get("Parameterization[PASSING beta: [b] ]"), OneFailingTestExample.class, "failing", true);
    }

    @Test
    public void testOverrideOfFailingParam() throws Throwable
    {
        workingProperties.put(NamedParameterizedRunner.PARAMETERIZATION_OVERRIDE, "beta");
        ParameterizationBuilder builder = new ParameterizationBuilder(builderList);
        builder.add("alpha", 'a');
        builder.addFailing("beta", 'b');

        NamedParameterizedRunner runner = new NamedParameterizedRunner(OneFailingTestExample.class);

        Map<String,ReifiedParamRunner> map = testParameterizations(runner,
                "Parameterization[FAILING beta: [b] ]");

        testOverrides(map.values(), true);

        testFrameworkMethod(map.get("Parameterization[FAILING beta: [b] ]"), OneFailingTestExample.class, "passing", true);
        testFrameworkMethod(map.get("Parameterization[FAILING beta: [b] ]"), OneFailingTestExample.class, "failing", true);
    }

    @Test
    public void testOverrideUsingRegex() throws Throwable
    {
        workingProperties.put(NamedParameterizedRunner.PARAMETERIZATION_OVERRIDE, "/a/");
        ParameterizationBuilder builder = new ParameterizationBuilder(builderList);
        builder.add("alpha", 'a');
        builder.addFailing("beta", 'b');

        NamedParameterizedRunner runner = new NamedParameterizedRunner(OneFailingTestExample.class);

        Map<String,ReifiedParamRunner> map = testParameterizations(runner,
                "Parameterization[PASSING alpha: [a] ]",
                "Parameterization[FAILING beta: [b] ]");

        testOverrides(map.values(), true);

        testFrameworkMethod(map.get("Parameterization[PASSING alpha: [a] ]"), OneFailingTestExample.class, "passing", true);
        testFrameworkMethod(map.get("Parameterization[PASSING alpha: [a] ]"), OneFailingTestExample.class, "failing", true);
        testFrameworkMethod(map.get("Parameterization[FAILING beta: [b] ]"), OneFailingTestExample.class, "passing", true);
        testFrameworkMethod(map.get("Parameterization[FAILING beta: [b] ]"), OneFailingTestExample.class, "failing", true);
    }

    @Test
    public void testOnlyIf() throws Throwable {
        ParameterizationBuilder builder = new ParameterizationBuilder(builderList);
        builder.add("one", 1);
        builder.add("two", 2);

        NamedParameterizedRunner runner = new NamedParameterizedRunner(OnlyIfTestExample.class);

        Map<String,ReifiedParamRunner> map = testParameterizations(runner,
                "Parameterization[PASSING one: [1] ]",
                "Parameterization[PASSING two: [2] ]");

        {
            ReifiedParamRunner forOne = map.get("Parameterization[PASSING one: [1] ]");
            assertEquals("param one: " + forOne.describeChildren(), 3, forOne.getChildrenCount());
            testFrameworkMethod(forOne, OnlyIfTestExample.class, "testIs1Method", true);
            testFrameworkMethod(forOne, OnlyIfTestExample.class, "testIs1Field", true);
            testFrameworkMethod(forOne, OnlyIfTestExample.class, "alwaysRun", true);
        }
        {
            ReifiedParamRunner forTwo = map.get("Parameterization[PASSING two: [2] ]");
            assertEquals("param two: " + forTwo.describeChildren(), 1, forTwo.getChildrenCount());
            testFrameworkMethod(forTwo, OnlyIfTestExample.class, "alwaysRun", true);
        }
    }

    private static void testOverrides(Collection<ReifiedParamRunner> runners, boolean expectedOverride)
    {
        for (ReifiedParamRunner runner : runners)
        {
            assertEquals(runner.toString(), expectedOverride, runner.overrideOn());
        }
    }

    private static void testFrameworkMethod(ReifiedParamRunner runner, Class<?> testClass, String testMethod,
                                            boolean expectedToPass)
    {
        String frameworkMethodString = "public void " + testClass.getName() + "." + testMethod + "()";
        FrameworkMethod method = runner.getChild(frameworkMethodString);
        assertNotNull(frameworkMethodString, method);
        assertEquals(frameworkMethodString, expectedToPass, runner.expectedToPass(method));
    }

    /**
     * Confirms that each given name has a {@linkplain ReifiedParamRunner} associated with it, and returns the
     * name -> runner map
     * @param runner the parameterized runner
     * @param names the expected names
     * @return a map of names to reified runners
     */
    private static Map<String,ReifiedParamRunner> testParameterizations(NamedParameterizedRunner runner, String... names)
    {
        List<Runner> children = runner.getChildren();
        assertEquals("children.size()", names.length, children.size());

        Set<String> expectedNames = new HashSet<>(names.length, 1.0f);
        for (String name : names) {
            assertTrue("unexpected error, duplicate name: " + name, expectedNames.add(name));
        }

        Map<String,ReifiedParamRunner> foundRunners = new HashMap<>();
        for (Runner child : children)
        {
            ReifiedParamRunner reified = (ReifiedParamRunner)child;
            String paramToString = reified.paramToString();
            assertNull("duplicate name: " + paramToString, foundRunners.put(paramToString, reified));
        }

        for (String expected : expectedNames)
        {
            assertTrue("didn't find expected param: " + expected, foundRunners.containsKey(expected));
        }

        return foundRunners;
    }
}
