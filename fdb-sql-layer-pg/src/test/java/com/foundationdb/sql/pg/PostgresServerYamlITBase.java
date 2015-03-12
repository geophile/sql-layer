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

import com.foundationdb.sql.test.YamlTester;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

/**
 * A base class for integration tests that use data from YAML files to specify the input and output expected from calls
 * to the Postgres server.  Subclasses should call {@link #testYaml} with the file to use for the test.
 */
public class PostgresServerYamlITBase extends PostgresServerITBase
{
    private static final Logger LOG = LoggerFactory.getLogger(PostgresServerYamlITBase.class);

    protected PostgresServerYamlITBase() {
    }

    protected boolean isRandomCost(){
        return false;
    }

    /** Run a test with YAML input from the specified URL. */
    protected void testYaml(URL url) throws Exception {
        LOG.debug("URL: {}", url);
        Throwable thrown = null;
        try(Reader in = new InputStreamReader(url.openStream(), "UTF-8")) {
            new YamlTester(url, in, getConnection(), isRandomCost())
                .test();
            LOG.debug("Test passed");
        } catch(Exception | AssertionError e) {
            thrown = e;
            throw e;
        } finally {
            if(thrown != null) {
                LOG.error("Test failed", thrown);
                try {
                    forgetConnection();
                } catch(Exception e2) {
                    // Ignore
                }
            }
        }
    }

}
