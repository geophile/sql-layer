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

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.text.FullTextIndexService;
import com.foundationdb.server.service.text.FullTextIndexServiceImpl;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.test.YamlTestFinder;

import java.net.URL;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Run tests specified as YAML files that end with the .yaml extension.  By
 * default, searches for files recursively in the yaml resource directory,
 * running tests for files that start with 'test-'.
 */
@RunWith(SelectedParameterizedRunner.class)
public class PostgresServerMiscYamlIT extends PostgresServerYamlITBase
{
    private final URL url;

    public PostgresServerMiscYamlIT(String caseName, URL url) {
        this.url = url;
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        // Get embedded JDBC and substitute working full text.
        return super.serviceBindingsProvider()
                .require(EmbeddedJDBCService.class)
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void testYaml() throws Exception {
        testYaml(url);
    }

    @Parameters(name="{0}")
    public static Iterable<Object[]> queries() throws Exception {
        return YamlTestFinder.findTests();
    }

}
