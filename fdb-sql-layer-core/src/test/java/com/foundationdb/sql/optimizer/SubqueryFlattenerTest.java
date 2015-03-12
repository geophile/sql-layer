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

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Collection;

@RunWith(SelectedParameterizedRunner.class)
public class SubqueryFlattenerTest extends OptimizerTestBase 
                                   implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "flatten");

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> statements() throws Exception {
        return sqlAndExpected(RESOURCE_DIR);
    }

    public SubqueryFlattenerTest(String caseName, String sql, 
                                 String expected, String error) {
        super(caseName, sql, expected, error);
    }

    @Before
    public void loadDDL() throws Exception {
        loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
        ((BoundNodeToString)unparser).setUseBindings(true);
    }

    @Test
    public void testFlatten() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        return unparser.toString(stmt);
    }

    @Override
    public void checkResult(String result) {
        assertEquals(caseName, expected.trim(), result);
    }

}
