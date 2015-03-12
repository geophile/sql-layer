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
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.FDBScanTransactionOptions;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.FDBStoreDataHelper;
import com.persistit.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.server.store.statistics.IndexStatisticsService.INDEX_STATISTICS_TABLE_NAME;
import static com.foundationdb.server.store.statistics.IndexStatisticsVisitor.VisitorCreator;

public class FDBStoreIndexStatistics extends AbstractStoreIndexStatistics<FDBStore> implements VisitorCreator<Key,byte[]> {
    public static final String SAMPLER_COUNT_LIMIT_PROPERTY = "fdbsql.index_statistics.sampler_count_limit";
    private static final Logger logger = LoggerFactory.getLogger(FDBStoreIndexStatistics.class);

    private final IndexStatisticsService indexStatisticsService;
    private final long samplerCountLimit;

    public FDBStoreIndexStatistics(FDBStore store, IndexStatisticsService indexStatisticsService, ConfigurationService configurationService) {
        super(store);
        this.indexStatisticsService = indexStatisticsService;
        this.samplerCountLimit = Long.parseLong(configurationService.getProperty(SAMPLER_COUNT_LIMIT_PROPERTY));
    }


    //
    // AbstractStoreIndexStatistics
    //

    @Override
    public IndexStatistics loadIndexStatistics(Session session, Index index) {
        Table indexStatisticsTable = getStore().getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        Table indexTable = index.leafMostTable();
        Schema schema = SchemaCache.globalSchema(getStore().getAIS(session));
        FDBStoreData storeData = getStore().createStoreData(session, indexStatisticsTable.getGroup());
        storeData.persistitKey.append(indexStatisticsTable.getOrdinal())
        .append((long) indexTable.getTableId())
        .append((long) index.getIndexId());

        IndexStatistics result = null;
        getStore().groupKeyAndDescendantsIterator(session, storeData, FDBScanTransactionOptions.SNAPSHOT);
        while(storeData.next()) {
            FDBStoreDataHelper.unpackKey(storeData);
            if(result == null) {
                result = decodeHeader(session, storeData, index, schema);
            } else {
                decodeEntry(session, storeData, result, schema);
            }
        }
        if ((result != null) && logger.isDebugEnabled()) {
            logger.debug("Loaded: " + result.toString(index));
        }
        return result;
    }

    @Override
    public void removeStatistics(Session session, Index index) {
        Table table = getStore().getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        Table indexTable = index.leafMostTable();
        FDBStoreData storeData = getStore().createStoreData(session, table.getGroup());
        storeData.persistitKey.clear();

        storeData.persistitKey.append(table.getOrdinal())
            .append((long) indexTable.getTableId())
            .append((long) index.getIndexId());
        getStore().groupKeyAndDescendantsIterator(session, storeData, FDBScanTransactionOptions.NORMAL);
        while(storeData.next()) {
            FDBStoreDataHelper.unpackKey(storeData);
            Row row = getStore().expandRow(session, storeData, SchemaCache.globalSchema(index.getAIS()));
            getStore().deleteRow(session, row, false);
        }
    }

    @Override
    public IndexStatistics computeIndexStatistics(Session session, Index index, long scanTimeLimit, long sleepTime) {
        FDBScanTransactionOptions transactionOptions;
        if (scanTimeLimit > 0) {
            transactionOptions = new FDBScanTransactionOptions(true, -1,
                                                               scanTimeLimit, sleepTime);
        }
        else {
            transactionOptions = FDBScanTransactionOptions.SNAPSHOT;
        }
        long indexRowCount = estimateIndexRowCount(session, index);
        long expectedSampleCount = indexRowCount;
        int sampleRate = 1, skippedSamples = 0;
        int nSingle = index.getKeyColumns().size() - 1;
        if (nSingle > 0) {
            // Multi-column index might need sampling.  In the worst case, the visitor
            // will need to retain one copy of the key for each non-leading column for
            // each sampled row. Keep that below samplerCountLimit by sampling every few
            // rows. We could still send everything for the leading column, except that
            // the sample count applies to the whole, not per histograms.
            sampleRate = (int)((indexRowCount * nSingle + samplerCountLimit - 1) / samplerCountLimit); // Round up.
            if (sampleRate > 1) {
                expectedSampleCount = indexRowCount / sampleRate;
                logger.debug("Sampling rate for {} is {}", index, sampleRate);
            }
        }
        IndexStatisticsVisitor<Key,byte[]> visitor = new IndexStatisticsVisitor<>(session, index, indexRowCount, expectedSampleCount, this);
        int bucketCount = indexStatisticsService.bucketCount();
        visitor.init(bucketCount);
        FDBStoreData storeData = getStore().createStoreData(session, index);
        // Whole index, forward.
        getStore().indexIterator(session, storeData, false, false, true, false,
                                 transactionOptions);
        while(storeData.next()) {
            if (++skippedSamples < sampleRate)
                continue;       // This value not sampled.
            skippedSamples = 0;
            FDBStoreDataHelper.unpackKey(storeData);
            // TODO: Does anything look at rawValue?
            visitor.visit(storeData.persistitKey, storeData.rawValue);
        }
        visitor.finish(bucketCount);
        IndexStatistics indexStatistics = visitor.getIndexStatistics();
        if (logger.isDebugEnabled()) {
            logger.debug("Analyzed: " + indexStatistics.toString(index));
        }
        return indexStatistics;
    }


    //
    // VisitorCreator
    //

    @Override
    public IndexStatisticsGenerator<Key,byte[]> multiColumnVisitor(Index index) {
        return new FDBMultiColumnIndexStatisticsVisitor(index, getStore());
    }

    @Override
    public IndexStatisticsGenerator<Key,byte[]> singleColumnVisitor(Session session, IndexColumn indexColumn) {
        return new FDBSingleColumnIndexStatisticsVisitor(getStore(), indexColumn);
    }


    //
    // Internal
    //

    protected IndexStatistics decodeHeader(Session session,
                                           FDBStoreData storeData,
                                           Index index, 
                                           Schema schema) {
        return decodeIndexStatisticsRow(getStore().expandRow(session, storeData, schema), index);
    }

    protected void decodeEntry(Session session,
                               FDBStoreData storeData,
                               IndexStatistics indexStatistics,
                               Schema schema) {
        decodeIndexStatisticsEntryRow(getStore().expandRow(session, storeData, schema), indexStatistics);
    }
}
