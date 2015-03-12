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
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.IndexVisitor;

import java.util.ArrayList;
import java.util.List;

public class IndexStatisticsVisitor<K extends Comparable<? super K>, V> extends IndexVisitor<K,V>
{
    public interface VisitorCreator<K extends Comparable<? super K>, V> {
        IndexStatisticsGenerator<K,V> multiColumnVisitor(Index index);
        IndexStatisticsGenerator<K,V> singleColumnVisitor(Session session, IndexColumn indexColumn);
    }

    public IndexStatisticsVisitor(Session session,
                                  Index index,
                                  long indexRowCount,
                                  long estimatedSampleCount,
                                  VisitorCreator<K,V> creator)
    {
        this.index = index;
        this.indexRowCount = indexRowCount;
        this.estimatedSampleCount = estimatedSampleCount;
        this.multiColumnVisitor = creator.multiColumnVisitor(index);
        this.nIndexColumns = index.getKeyColumns().size();
        this.singleColumnVisitors = new ArrayList<>(nIndexColumns-1);
        // Single column 0 is handled as leading column of multi-column.
        for (int f = 1; f < nIndexColumns; f++) {
            singleColumnVisitors.add(
                    creator.singleColumnVisitor(session, index.getKeyColumns().get(f))
            );
        }
    }

    public void init(int bucketCount)
    {
        multiColumnVisitor.init(bucketCount, estimatedSampleCount);
        for (int c = 1; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c-1).init(bucketCount, estimatedSampleCount);
        }
    }

    public void finish(int bucketCount)
    {
        multiColumnVisitor.finish(bucketCount);
        for (int c = 1; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c-1).finish(bucketCount);
        }
    }

    protected void visit(K key, V value)
    {
        multiColumnVisitor.visit(key, value);
        for (int c = 1; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c-1).visit(key, value);
        }
    }

    public IndexStatistics getIndexStatistics()
    {
        IndexStatistics indexStatistics = new IndexStatistics(index);
        // The multi-column visitor has the sampled row count. The single-column visitors
        // have the count of distinct sampled keys for that column.
        int sampledCount = multiColumnVisitor.rowCount();
        indexStatistics.setRowCount(indexRowCount);
        indexStatistics.setSampledCount(sampledCount);
        multiColumnVisitor.getIndexStatistics(indexStatistics);
        for (int c = 1; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c-1).getIndexStatistics(indexStatistics);
        }
        return indexStatistics;
    }

    private final Index index;
    private final long indexRowCount, estimatedSampleCount;
    private final IndexStatisticsGenerator<K,V> multiColumnVisitor;
    private final List<IndexStatisticsGenerator<K,V>> singleColumnVisitors;
    private final int nIndexColumns;
}
