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

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.runner.Runner;
import org.junit.runners.Parameterized;

public final class SelectedParameterizedRunner extends Parameterized {

    /**
     * <p>Parameterization override filter (works by name).</p>
     *
     * <p>If this property is set, then only parameterization names that match its value will be processed. These
     * names behave like @Failing names (in terms of regexes, etc). If this property is set and a test that matches
     * it is marked as @Failing, that test will still get run. For instance, if a parameterization named
     * <tt>myFooTest</tt> is marked as failing for a given test (either because the entire parameterization is marked
     * as failing, or because of a <tt>@Failing</tt> annotation on the method), and if you have a system property
     * <tt>{@value} == "/myFoo/"</tt>, then the test <em>will</em> be run.
     */
    public final static String PARAMETERIZATION_OVERRIDE = "fdbsql.test.param.override";
    
    private final String override;
    private final boolean overrideIsRegex;

    private final static Logger logger = LoggerFactory.getLogger(SelectedParameterizedRunner.class);
    
    public SelectedParameterizedRunner(Class<?> clazz) throws Throwable {
        super(clazz);

        override = System.getProperty(PARAMETERIZATION_OVERRIDE);
        overrideIsRegex = (override != null) && paramNameUsesRegex(override);
        if (override != null) {
            String msg = "Override is set to";
            if (overrideIsRegex) {
                msg += " regex";
            }
            msg += ":" + override;
            logger.debug(msg);
        }
    }

    static boolean paramNameUsesRegex(String paramName) {
        return paramName.length() > 2
                && (paramName.charAt(0)=='/')
                && (paramName.charAt(paramName.length()-1)=='/');
    }
    
    /**
     * Returns whether a given parameterization matches a given regex. The regex should be in "/regex/" format.
     * @param paramName the haystack, as it were
     * @param paramRegex a string that starts and ends with '/', and between them has a needle.
     * @return whether the paramRegex is found in paramName
     */
    static boolean paramNameMatchesRegex(String paramName, String paramRegex) {
        assert paramRegex.charAt(0)=='/';
        assert paramRegex.charAt(paramRegex.length()-1)=='/';
        assert paramRegex.length() > 2;
        String regex = paramRegex.substring(1, paramRegex.length()-1);
        return Pattern.compile(regex).matcher(paramName).find();
    }
    
    @Override
    protected List<Runner> getChildren() {
        List<Runner> children = super.getChildren();
        
        if (override != null) {
            for (Iterator<Runner> iterator = children.iterator(); iterator.hasNext(); ) {
                Runner child = iterator.next();
                String fName = child.getDescription().getDisplayName();
                if (fName.startsWith("[") && fName.endsWith("]")) {
                    fName = fName.substring(1, fName.length()-1);
                }
                if (overrideIsRegex && !paramNameMatchesRegex(fName, override)) {
                    iterator.remove();
                }
                else if (!overrideIsRegex && !fName.equals(override)) {
                    iterator.remove();
                }
            }
        }        
        return children;
    }
}
