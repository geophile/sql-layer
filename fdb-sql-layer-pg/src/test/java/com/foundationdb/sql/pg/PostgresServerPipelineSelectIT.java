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
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RunWith(NamedParameterizedRunner.class)
public class PostgresServerPipelineSelectIT extends PostgresServerSelectIT 
{
    public static final File RESOURCE_DIR = 
        new File(PostgresServerITBase.RESOURCE_DIR, "pipeline-select");

    @Override
    protected Map<String, String> startupConfigProperties() {
        Properties loadedProperties = new Properties();
        try {
            FileInputStream istr = new FileInputStream(new File(RESOURCE_DIR,
                                                                "pipeline.properties"));
            loadedProperties.load(istr);
            istr.close();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        Map<String, String> properties = new HashMap<>();
        for (String key : loadedProperties.stringPropertyNames()) {
            properties.put(key, loadedProperties.getProperty(key));
        }
        return properties;
    }

    @Override
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        return NamedParamsTestBase.namedCases(TestBase.sqlAndExpectedAndParams(RESOURCE_DIR));
    }

    public PostgresServerPipelineSelectIT(String caseName, String sql, 
                                          String expected, String error,
                                          String[] params) {
        super(caseName, sql, expected, error, params);
    }

}
