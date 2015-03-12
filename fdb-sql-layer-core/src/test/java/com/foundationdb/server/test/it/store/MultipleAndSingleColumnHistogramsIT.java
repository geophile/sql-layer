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
package com.foundationdb.server.test.it.store;

import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.store.statistics.Histogram;
import com.foundationdb.server.store.statistics.HistogramEntry;
import com.foundationdb.server.store.statistics.IndexStatistics;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MultipleAndSingleColumnHistogramsIT extends ITBase
{
    @Before
    public void createDatabase()
    {
        int table = createTable(SCHEMA, TABLE,
                                "id int not null",
                                "a int",
                                "b int",
                                "c int",
                                "primary key(id)");
        index = createIndex(SCHEMA, TABLE, INDEX, "a", "b", "c");
        int a = 0;
        int b = 0;
        int c = 0;
        for (int id = 0; id < N; id++) {
            writeRow(table, id, a, b, c);
            a = (a + 1) % A_COUNT;
            b = (b + 1) % B_COUNT;
            c = (c + 1) % C_COUNT;
        }
        ddl().updateTableStatistics(session(),
                                    TableName.create(SCHEMA, TABLE),
                                    Collections.singleton(index.getIndexName().getName()));
    }

    @Test
    public void test()
    {
        IndexStatisticsService statsService = statsService();
        IndexStatistics stats = statsService.getIndexStatistics(session(), index);
        Histogram histogram;
        List<HistogramEntry> entries;
        // Multi-column histogram on (a)
        histogram = stats.getHistogram(0, 1);
        assertEquals(0, histogram.getFirstColumn());
        assertEquals(1, histogram.getColumnCount());
        assertEquals(A_COUNT, histogram.totalDistinctCount());
        entries = histogram.getEntries();
        int a = 0;
        for (HistogramEntry entry : entries) {
            assertEquals(String.format("{(long)%s}", a++), entry.getKeyString());
            assertEquals(N / A_COUNT, entry.getEqualCount());
            assertEquals(0, entry.getDistinctCount());
            assertEquals(0, entry.getLessCount());
        }
        // Not checking other multi-column entries in detail, because there's nothing new, and they're pretty useless.
        // Single-column histogram on (b)
        histogram = stats.getHistogram(1, 1);
        assertEquals(1, histogram.getFirstColumn());
        assertEquals(1, histogram.getColumnCount());
        assertEquals(B_COUNT, histogram.totalDistinctCount());
        entries = histogram.getEntries();
        int b = 0;
        for (HistogramEntry entry : entries) {
            assertEquals(String.format("{(long)%s}", b++), entry.getKeyString());
            assertEquals(N / B_COUNT, entry.getEqualCount());
            assertEquals(0, entry.getDistinctCount());
            assertEquals(0, entry.getLessCount());
        }
        // Single-column histogram on (c)
        histogram = stats.getHistogram(2, 1);
        assertEquals(2, histogram.getFirstColumn());
        assertEquals(1, histogram.getColumnCount());
        assertEquals(C_COUNT, histogram.totalDistinctCount());
        entries = histogram.getEntries();
        int c = 0;
        for (HistogramEntry entry : entries) {
            assertEquals(String.format("{(long)%s}", c++), entry.getKeyString());
            assertEquals(N / C_COUNT, entry.getEqualCount());
            assertEquals(0, entry.getDistinctCount());
            assertEquals(0, entry.getLessCount());
        }
    }

    private IndexStatisticsService statsService()
    {
        return serviceManager().getServiceByClass(IndexStatisticsService.class);
    }

    private static final String SCHEMA = "schema";
    private static final String TABLE = "t";
    private static final String INDEX = "idx_abc";
    private static final int N = 1000;
    private static final int A_COUNT = 5;
    private static final int B_COUNT = 10;
    private static final int C_COUNT = 25;

    private TableIndex index;
}
