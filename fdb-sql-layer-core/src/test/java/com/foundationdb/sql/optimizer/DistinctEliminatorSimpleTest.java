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

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class DistinctEliminatorSimpleTest extends DistinctEliminatorTestBase {

    private static final File SIMPLE_TEST = new File(RESOURCE_DIR, "simple-distincts.yaml");

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        FileReader fileReader = new FileReader(SIMPLE_TEST);
        try {
            ParameterizationBuilder pb = new ParameterizationBuilder();
            BufferedReader buffered = new BufferedReader(fileReader);
            Yaml yaml = new Yaml();
            for (Object objRaw : yaml.loadAll(buffered)) {
                List<?> asList = (List<?>) objRaw;
                for (Object lineRaw : asList) {
                    Map<?,?> line = (Map<?,?>) lineRaw;
                    if (line.size() != 1)
                        throw new RuntimeException("need key-val pair:" + line);
                    String actionStr = (String) line.keySet().iterator().next();
                    String sql = (String) line.get(actionStr);

                    String name = sql;
                    if (name.startsWith("SELECT DISTINCT"))
                        name = name.substring("SELECT DISTINCT".length());

                    KeepOrOptimize action = KeepOrOptimize.valueOf(actionStr.toUpperCase());
                    pb.create(name, action != KeepOrOptimize.IGNORED, sql,  action);
                }
            }
            return pb.asList();
        }
        finally {
            fileReader.close();
        }
    }

    @Test
    public void test() throws Exception {
        if (!sql.toUpperCase().contains("DISTINCT"))
            throw new RuntimeException("original didn't have DISTINCT");
        String optimized = optimized();
        KeepOrOptimize distinctActualOptimized = optimized.contains("DISTINCT")
                ? KeepOrOptimize.KEPT
                : KeepOrOptimize.OPTIMIZED;
        assertEquals(optimized, distinctExpectedOptimized, distinctActualOptimized);
        
    }

    public DistinctEliminatorSimpleTest(String sql, KeepOrOptimize distinctExpectedOptimized) {
        super(sql, sql, null, null);
        this.distinctExpectedOptimized = distinctExpectedOptimized;
    }
    
    public final KeepOrOptimize distinctExpectedOptimized;
    
    private enum KeepOrOptimize {
        KEPT, OPTIMIZED, IGNORED
    }
}
