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

import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import java.sql.PreparedStatement;

import java.io.File;
import java.util.Collection;

@RunWith(NamedParameterizedRunner.class)
public class PostgresServerUpdateIT extends PostgresServerFilesITBase 
                                    implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(PostgresServerITBase.RESOURCE_DIR, "update");

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        return NamedParamsTestBase.namedCases(TestBase.sqlAndExpectedAndParams(RESOURCE_DIR));
    }

    public PostgresServerUpdateIT(String caseName, String sql, 
                                  String expected, String error,
                                  String[] params) {
        super(caseName, sql, expected, error, params);
    }

    @Test
    public void testUpdate() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        PreparedStatement stmt = getConnection().prepareStatement(sql);
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                String param = params[i];
                if (param.startsWith("#"))
                    stmt.setLong(i + 1, Long.parseLong(param.substring(1)));
                else
                    stmt.setString(i + 1, param);
            }
        }
        try {
            int count = stmt.executeUpdate();
        }
        catch (Exception ex) {
            if (error == null)
                forgetConnection();
            throw ex;
        }
        stmt.close();
        return dumpData();
    }

    @Override
    public void checkResult(String result) {
        assertEquals(caseName, expected, result);
    }

}
