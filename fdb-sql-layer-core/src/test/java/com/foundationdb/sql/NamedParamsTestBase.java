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
package com.foundationdb.sql;

import com.foundationdb.junit.Parameterization;

import org.junit.Ignore;

import java.util.Collection;
import java.util.ArrayList;

@Ignore
public class NamedParamsTestBase extends TestBase
{
    protected NamedParamsTestBase() {
    }

    protected NamedParamsTestBase(String caseName, String sql, String expected, String error) {
      super(caseName, sql, expected, error);
    }

    /** Given method args whose first one is caseName, make named parameterizations. */
    public static Collection<Parameterization> namedCases(Collection<Object[]> margs) {
        Collection<Parameterization> result = new ArrayList<>(margs.size());
        for (Object[] args : margs) {
            String caseName = (String)args[0];
            result.add(Parameterization.create(caseName, args));
        }
        return result;
    }

}
