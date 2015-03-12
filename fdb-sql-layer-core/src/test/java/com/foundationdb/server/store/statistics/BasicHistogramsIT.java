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
package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.service.config.TestConfigService;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.util.Strings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public class BasicHistogramsIT extends ITBase
{
    private static final String SCHEMA = "test";
    private static final File RESOURCE_DIR = new File("src/test/resources/" +
                                                      BasicHistogramsIT.class.getPackage().getName().replace('.', '/') +
                                                      "/histograms");

    private final String bucketCount;
    private final File expectedFile;

    @TestParameters
    public static Collection<Parameterization> types() throws Exception {
        String[] sizes = { "32", "256" };
        Parameterization[] params = new Parameterization[sizes.length];
        for(int i = 0; i < sizes.length; ++i) {
            params[i] = Parameterization.create("buckets_"+sizes[i],
                                                sizes[i],
                                                "stats_"+sizes[i]+".yaml");
        }
        return Arrays.asList(params);
    }

    public BasicHistogramsIT(String bucketCount, String expectedFile) {
        this.bucketCount = bucketCount;
        this.expectedFile = new File(RESOURCE_DIR, expectedFile);
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put(TestConfigService.BUCKET_COUNT_KEY, bucketCount);
        return config;
    }

    @Before
    public void load() throws Exception {
        loadDatabase(SCHEMA, RESOURCE_DIR);
    }

    @Test
    public void test() throws IOException {
        final Set<Index> indexes = new HashSet<>();
        for(Table t : ais().getSchema(SCHEMA).getTables().values()) {
            indexes.addAll(t.getIndexes());
            indexes.addAll(t.getGroup().getIndexes());

        }
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                indexStatsService().updateIndexStatistics(session(), indexes);
            }
        });
                
        // The read of index statistics is done through a snapshot view,
        // so commit the changes before trying to read them. 
        String actual = txnService().run(session(), new Callable<String>() {
            @Override
            public String call() throws IOException {
                StringWriter writer = new StringWriter();
                indexStatsService().dumpIndexStatistics(session(), SCHEMA, writer);
                return writer.toString();
            }
        });
        String expected = Strings.dumpFileToString(expectedFile);
        assertEquals(strip(expected), strip(actual));
    }

    private String strip(String s) {
        return s.replace("\r", "").trim().replaceAll("Timestamp: .*Z", "Timestamp: null");
    }
}
