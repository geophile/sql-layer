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
package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

@RunWith(NamedParameterizedRunner.class)
public class AISBinderTest extends OptimizerTestBase 
                           implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "binding");

    @TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        return NamedParamsTestBase.namedCases(sqlAndExpected(RESOURCE_DIR));
    }

    public AISBinderTest(String caseName, String sql, String expected, String error) {
        super(caseName, sql, expected, error);
    }

    @Test
    public void testBinding() throws Exception {
        loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
        File propFile = new File(RESOURCE_DIR, caseName + ".properties");
        if (propFile.exists()) {
            Properties properties = new Properties();
            try (FileInputStream str = new FileInputStream(propFile)) {
                properties.load(str);
            }
            for (String prop : properties.stringPropertyNames()) {
                if ("allowSubqueryMultipleColumns".equals(prop)) {
                    binder.setAllowSubqueryMultipleColumns(Boolean.parseBoolean(properties.getProperty(prop)));
                }
                else if ("resultColumnsAvailableBroadly".equals(prop)) {
                    binder.setResultColumnsAvailableBroadly(Boolean.parseBoolean(properties.getProperty(prop)));
                }
                else {
                    throw new Exception("Unknown binding property: " + prop);
                }
            }
        }
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        return getTree(stmt);
    }
    
    @Override
    public void checkResult(String result) throws IOException {
        assertEqualsWithoutHashes(caseName,
                expected.replaceAll(":\\s+\n",":\n"),
                result.replaceAll(":\\s+\n",":\n"));
    }

}
