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
package com.foundationdb.sql.server;

import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.statistics.IndexStatistics;
import com.foundationdb.server.store.statistics.IndexStatisticsService;

public class ServerCostEstimator extends CostEstimator
{
    private ServerSession session;
    private IndexStatisticsService indexStatistics;
    private boolean scaleIndexStatistics;
    private boolean testMode;

    public ServerCostEstimator(ServerSession session,
                               ServerServiceRequirements reqs,
                               ServerOperatorCompiler compiler, KeyCreator keyCreator) {
        super(compiler, keyCreator, reqs.costModel());
        this.session = session;
        indexStatistics = reqs.indexStatistics();
        scaleIndexStatistics = Boolean.parseBoolean(getProperty("scaleIndexStatistics", "true"));
        testMode = reqs.config().testing();
    }

    @Override
    public IndexStatistics getIndexStatistics(Index index) {
        return indexStatistics.getIndexStatistics(session.getSession(), index);
    }

    @Override
    public long getTableRowCount(Table table) {
        if (!scaleIndexStatistics) {
            // Unscaled test mode: return count from statistics, if present.
            long count = getTableRowCountFromStatistics(table);
            if (count >= 0)
                return count;
        }
        return table.tableStatus().getApproximateRowCount(session.getSession());
    }

    @Override
    protected void missingStats(Index index, Column column) {
        if (!testMode) {
            indexStatistics.missingStats(session.getSession(), index, column);
        }
    }

    @Override
    protected void checkRowCountChanged(Table table, IndexStatistics stats, long rowCount) {
        if (!testMode) {
            indexStatistics.checkRowCountChanged(session.getSession(), table, stats, rowCount);
        }
    }

}
