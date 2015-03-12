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
package com.foundationdb.sql.pg;

import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Ignore
public abstract class PostgresServerBinaryITBase extends PostgresServerSelectIT
{
    public PostgresServerBinaryITBase(String caseName, String sql, String expected, String error, String[] params) {
        super(caseName, sql, expected, error, params);
    }

    protected abstract boolean isBinaryTransfer();

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        Collection<Object[]> allCases = TestBase.sqlAndExpectedAndParams(RESOURCE_DIR);
        List<Object[]> typeCases = new ArrayList<>();
        for(Object[] a : allCases) {
            String caseName = (String)a[0];
            if(caseName.equals("types") || caseName.startsWith("types_a")) {
                typeCases.add(a);
            }
        }
        assert !typeCases.isEmpty() : "No types_a cases";
        return NamedParamsTestBase.namedCases(typeCases);
    }

    @Override
    protected String getConnectionURL() {
        // loglevel=2 is also useful for seeing what's really happening.
        return super.getConnectionURL() + "?prepareThreshold=1&binaryTransfer=" + isBinaryTransfer();
    }

    @Override
    protected boolean executeTwice() {
        return true;
    }

}
