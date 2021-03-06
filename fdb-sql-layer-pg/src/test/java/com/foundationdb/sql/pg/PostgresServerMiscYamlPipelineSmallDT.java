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

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/** Intended to test the opposite of default settings */
public class PostgresServerMiscYamlPipelineSmallDT extends PostgresServerMiscYamlIT {

    public final static String[] PIPELINE_PROPERTIES = {"fdbsql.pipeline.map.enabled=true",
                                                        "fdbsql.pipeline.indexScan.lookaheadQuantum=2",
                                                        "fdbsql.pipeline.groupLookup.lookaheadQuantum=2",
                                                        "fdbsql.pipeline.unionAll.openBoth=true",
                                                        "fdbsql.pipeline.selectBloomFilter.enabled=true"};

    public PostgresServerMiscYamlPipelineSmallDT(String caseName, URL url) {
        super(caseName, url);
    }
    
    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> superProperties = super.startupConfigProperties();
        Map<String, String> properties = new HashMap<>();
        for (String property : PIPELINE_PROPERTIES) {
            String[] pieces = property.split("=");
            properties.put(pieces[0], pieces[1]);
        }
        properties.putAll(superProperties);
        return properties;
    }
}
