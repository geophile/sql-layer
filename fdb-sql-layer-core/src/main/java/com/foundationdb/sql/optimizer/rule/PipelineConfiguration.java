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
package com.foundationdb.sql.optimizer.rule;

import java.util.Properties;

public class PipelineConfiguration
{
    private boolean mapEnabled = false;
    private int indexScanLookaheadQuantum = 1;
    private int groupLookupLookaheadQuantum = 1;
    private boolean unionAllOpenBoth = false;
    private boolean selectBloomFilterEnabled = false;

    public PipelineConfiguration() {
    }

    public PipelineConfiguration(Properties properties) {
        load(properties);
    }

    public boolean isMapEnabled() {
        return mapEnabled;
    }

    public int getIndexScanLookaheadQuantum() {
        return indexScanLookaheadQuantum;
    }

    public int getGroupLookupLookaheadQuantum() {
        return groupLookupLookaheadQuantum;
    }

    public boolean isUnionAllOpenBoth() {
        return unionAllOpenBoth;
    }

    public boolean isSelectBloomFilterEnabled() {
        return selectBloomFilterEnabled;
    }

    public void load(Properties properties) {
        for (String prop : properties.stringPropertyNames()) {
            String val = properties.getProperty(prop);
            if ("map.enabled".equals(prop))
                mapEnabled = Boolean.parseBoolean(val);
            else if ("indexScan.lookaheadQuantum".equals(prop))
                indexScanLookaheadQuantum = Integer.parseInt(val);
            else if ("groupLookup.lookaheadQuantum".equals(prop))
                groupLookupLookaheadQuantum = Integer.parseInt(val);
            else if ("unionAll.openBoth".equals(prop))
                unionAllOpenBoth = Boolean.parseBoolean(val);
            else if ("selectBloomFilter.enabled".equals(prop))
                selectBloomFilterEnabled = Boolean.parseBoolean(val);
            else
                throw new IllegalArgumentException("Unknown property " + prop);
        }
    }
}
