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

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.MemoryITBase;
import org.junit.Assume;

import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Run yamls with Memory storage. */
public class PostgresServerMemoryStoreYamlDT extends PostgresServerMiscYamlIT
{
    // These require features not present with memory storage
    // and are not easy to suppress via yaml
    private static final Set<String> SKIP_NAMES = new HashSet<>(Arrays.asList(
        "test-alter-column-keys",
        "test-alter-table-add-index",
        "test-create-table",
        "test-fdb-column-keys-format",
        "test-fdb-delayed-foreign-key",
        "test-fdb-delayed-uniqueness",
        "test-fdb-tuple-format",
        "test-show-param",
        "test-storage-format",
        "test-transaction-isolation-level",
        "test-pg-readonly4"
    ));

    public PostgresServerMemoryStoreYamlDT(String caseName, URL url) {
        super(caseName, url);
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return MemoryITBase.doBind(super.serviceBindingsProvider());
    }

    @Override
    protected void testYaml(URL url) throws Exception {
        for(String skip : SKIP_NAMES) {
            Assume.assumeFalse("Skipped", url.getPath().contains(skip));
        }
        super.testYaml(url);
    }
}
