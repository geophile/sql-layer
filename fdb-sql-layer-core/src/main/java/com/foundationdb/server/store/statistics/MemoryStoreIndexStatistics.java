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
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.MemoryStore;
import com.foundationdb.server.store.MemoryStoreData;
import com.persistit.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.server.store.statistics.IndexStatisticsService.INDEX_STATISTICS_TABLE_NAME;
import static com.foundationdb.server.store.statistics.IndexStatisticsVisitor.VisitorCreator;

public class MemoryStoreIndexStatistics extends AbstractStoreIndexStatistics<MemoryStore> implements VisitorCreator<Key,byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryStoreIndexStatistics.class);

    private final IndexStatisticsService indexStatisticsService;

    public MemoryStoreIndexStatistics(MemoryStore store, IndexStatisticsService indexStatisticsService) {
        super(store);
        this.indexStatisticsService = indexStatisticsService;
    }

    //
    // AbstractStoreIndexStatistics
    //

    @Override
    public IndexStatistics loadIndexStatistics(Session session, Index index) {
        Table indexStatisticsTable = getStore().getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        Table indexTable = index.leafMostTable();
        Schema schema = SchemaCache.globalSchema(getStore().getAIS(session));
        MemoryStoreData storeData = getStore().createStoreData(session, indexStatisticsTable.getGroup());
        storeData.persistitKey.append(indexStatisticsTable.getOrdinal())
                              .append((long)indexTable.getTableId())
                              .append((long)index.getIndexId());
        IndexStatistics result = null;
        getStore().groupKeyAndDescendantsIterator(session, storeData);
        while(storeData.next()) {
            if(result == null) {
                result = decodeHeader(storeData, index, schema);
            } else {
                decodeEntry(storeData, result, schema);
            }
        }
        if((result != null) && LOG.isDebugEnabled()) {
            LOG.debug("Loaded: {}", result.toString(index));
        }
        return result;
    }

    @Override
    public void removeStatistics(Session session, Index index) {
        Table table = getStore().getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        Table indexTable = index.leafMostTable();
        MemoryStoreData storeData = getStore().createStoreData(session, table.getGroup());
        storeData.persistitKey.clear();
        storeData.persistitKey.append(table.getOrdinal())
                              .append((long)indexTable.getTableId())
                              .append((long)index.getIndexId());
        getStore().groupKeyAndDescendantsIterator(session, storeData);
        while(storeData.next()) {
            Row row = getStore().expandRow(session, storeData, SchemaCache.globalSchema(index.getAIS()));
            getStore().deleteRow(session, row, false);
        }
    }

    @Override
    public IndexStatistics computeIndexStatistics(Session session, Index index, long scanTimeLimit, long sleepTime) {
        long indexRowCount = estimateIndexRowCount(session, index);
        IndexStatisticsVisitor<Key,byte[]> visitor = new IndexStatisticsVisitor<>(session,
                                                                                  index,
                                                                                  indexRowCount,
                                                                                  indexRowCount /*expectedCount*/,
                                                                                  this);
        int bucketCount = indexStatisticsService.bucketCount();
        visitor.init(bucketCount);
        MemoryStoreData storeData = getStore().createStoreData(session, index);
        // Whole index, forward.
        getStore().indexIterator(session, storeData, false);
        while(storeData.next()) {
            MemoryStore.unpackKey(storeData);
            visitor.visit(storeData.persistitKey, storeData.rawValue);
        }
        visitor.finish(bucketCount);
        IndexStatistics indexStatistics = visitor.getIndexStatistics();
        if(LOG.isDebugEnabled()) {
            LOG.debug("Analyzed: " + indexStatistics.toString(index));
        }
        return indexStatistics;
    }

    //
    // VisitorCreator
    //

    @Override
    public IndexStatisticsGenerator<Key,byte[]> multiColumnVisitor(Index index) {
        return new MemoryMultiColumnIndexStatisticsVisitor(index, getStore());
    }

    @Override
    public IndexStatisticsGenerator<Key,byte[]> singleColumnVisitor(Session session, IndexColumn indexColumn) {
        return new MemorySingleColumnIndexStatisticsVisitor(getStore(), indexColumn);
    }

    //
    // Internal
    //

    protected IndexStatistics decodeHeader(MemoryStoreData storeData, Index index, Schema schema) {
        return decodeIndexStatisticsRow(getStore().expandRow(storeData.session, storeData, schema), index);
    }

    protected void decodeEntry(MemoryStoreData storeData, IndexStatistics indexStatistics, Schema schema) {
        decodeIndexStatisticsEntryRow(getStore().expandRow(storeData.session, storeData, schema), indexStatistics);
    }
}
