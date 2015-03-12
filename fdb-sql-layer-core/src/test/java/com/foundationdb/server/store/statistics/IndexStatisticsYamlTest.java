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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.collation.TestKeyCreator;

import static com.foundationdb.sql.TestBase.*;
import static com.foundationdb.sql.optimizer.OptimizerTestBase.*;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.StringWriter;
import java.util.*;
import java.io.File;

public class IndexStatisticsYamlTest
{
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + IndexStatisticsYamlTest.class.getPackage().getName().replace('.', '/'));
    public static final File SCHEMA_FILE = new File(RESOURCE_DIR, "schema.ddl");
    public static final File YAML_FILE = new File(RESOURCE_DIR, "stats.yaml");
    
    private AkibanInformationSchema ais;

    @Before
    public void loadSchema() throws Exception {
        ais = parseSchema(SCHEMA_FILE);
    }    

    @Test
    public void testLoadDump() throws Exception {
        Schema schema = SchemaCache.globalSchema(ais);
        IndexStatisticsYamlLoader loader = new IndexStatisticsYamlLoader(ais, "test", new TestKeyCreator(schema));
        Map<Index,IndexStatistics> stats = loader.load(YAML_FILE);
        File tempFile = File.createTempFile("stats", ".yaml");
        tempFile.deleteOnExit();
        StringWriter tempWriter = new StringWriter();
        loader.dump(stats, tempWriter);
        assertEquals("dump matches load", 
                     fileContents(YAML_FILE).replace("\r", ""),
                     tempWriter.toString().replace("\r", ""));
    }

}
