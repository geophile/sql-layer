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

import com.foundationdb.server.SchemaFactory;
import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.ais.model.AkibanInformationSchema;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

@RunWith(NamedParameterizedRunner.class)
public class BoundNodeToStringTest extends NamedParamsTestBase
                                   implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR =
        new File(OptimizerTestBase.RESOURCE_DIR, "unparser");

    protected SQLParser parser;
    protected BoundNodeToString unparser;
    protected AISBinder binder;

    @Before
    public void before() throws Exception {
        parser = new SQLParser();
        unparser = new BoundNodeToString();
        unparser.setUseBindings(true);

        String sql = fileContents(new File(RESOURCE_DIR, "schema.ddl"));
        SchemaFactory schemaFactory = new SchemaFactory(OptimizerTestBase.DEFAULT_SCHEMA);
        AkibanInformationSchema ais = schemaFactory.ais(sql);
        binder = new AISBinder(ais, OptimizerTestBase.DEFAULT_SCHEMA);
    }

    @TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        return namedCases(sqlAndExpected(RESOURCE_DIR));
    }

    public BoundNodeToStringTest(String caseName, String sql, 
                                 String expected, String error) {
        super(caseName, sql, expected, error);
    }

    @Test
    public void testBound() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        return unparser.toString(stmt);
    }

    @Override
    public void checkResult(String result) throws IOException {
        assertEquals(caseName, expected, result);
    }

}
