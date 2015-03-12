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

import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.store.statistics.IndexStatisticsYamlTest;
import static com.foundationdb.sql.TestBase.*;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.StringWriter;
import java.io.File;
import java.util.concurrent.Callable;

public class IndexStatisticsServiceIT extends PostgresServerFilesITBase
{
    public static final File DB_DIR = IndexStatisticsLifecycleIT.RESOURCE_DIR;
    public static final String YAML_FILE = IndexStatisticsYamlTest.class.getPackage().getName().replace('.', '/') + "/" + "stats.yaml";
    
    private IndexStatisticsService service;

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(DB_DIR);
    }

    @Before
    public void getService() throws Exception {
        service = serviceManager().getServiceByClass(IndexStatisticsService.class);
    }
    
    @Test
    public void testLoadDump() throws Exception {
        final File yamlFile = copyResourceToTempFile("/" + YAML_FILE);

        // The index statistics are now snapshot reads, meaning
        // you can no longer do an insert and read in the same
        // transaction. 
        transactionallyUnchecked(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Load(yamlFile);
                return null;
            }
        });
        
        
        transactionallyUnchecked(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Dump(yamlFile);
                return null;
            }
        });
    }

    public void Load(File yamlFile) throws Exception {
        service.loadIndexStatistics(session(), SCHEMA_NAME, yamlFile);
        service.clearCache();
    }

    public void Dump(File yamlFile) throws Exception {
        File tempFile = File.createTempFile("stats", ".yaml");
        tempFile.deleteOnExit();
        StringWriter tempWriter = new StringWriter();
        service.dumpIndexStatistics(session(), SCHEMA_NAME, tempWriter);
        assertEquals("dump matches load", 
                     fileContents(yamlFile).replace("\r", ""),
                tempWriter.toString().replace("\r", ""));
    }

}
