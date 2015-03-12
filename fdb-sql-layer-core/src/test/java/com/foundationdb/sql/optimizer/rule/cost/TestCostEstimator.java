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
package com.foundationdb.sql.optimizer.rule.cost;

import com.foundationdb.ais.model.*;
import com.foundationdb.sql.optimizer.OptimizerTestBase;

import com.foundationdb.qp.rowtype.Schema;

import com.foundationdb.server.collation.TestKeyCreator;
import com.foundationdb.server.store.statistics.IndexStatistics;
import com.foundationdb.server.store.statistics.IndexStatisticsYamlLoader;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TestCostEstimator extends CostEstimator
{
    private final AkibanInformationSchema ais;
    private final Map<Index,IndexStatistics> stats;

    public static class TestCostModelFactory implements CostModelFactory {
        @Override
        public CostModel newCostModel(Schema schema, TableRowCounts tableRowCounts) {
            // NOTE: For now, we use the Persistit model since that is how all the
            // existing tests were computed.
            return new PersistitCostModel(schema, tableRowCounts);
        }
    }

    public TestCostEstimator(AkibanInformationSchema ais, Schema schema, 
                             File statsFile, boolean statsIgnoreMissingIndexes,
                             Properties properties)
            throws IOException {
        super(schema, properties, new TestKeyCreator(schema), new TestCostModelFactory());
        this.ais = ais;
        if (statsFile == null)
            stats = Collections.<Index,IndexStatistics>emptyMap();
        else
            stats = new IndexStatisticsYamlLoader(ais, OptimizerTestBase.DEFAULT_SCHEMA, new TestKeyCreator(schema))
                .load(statsFile, statsIgnoreMissingIndexes);
    }

    @Override
    public IndexStatistics getIndexStatistics(Index index) {
        return stats.get(index);
    }

}
