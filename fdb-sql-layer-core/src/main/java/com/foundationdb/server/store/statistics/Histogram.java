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

import java.util.List;

public class Histogram
{
    @Override
    public String toString()
    {
        return toString(null);
    }

    public String toString(Index index)
    {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        if (index != null) {
            str.append(" for ").append(index.getIndexName()).append("(");
            for (int j = 0; j < columnCount; j++) {
                if (j > 0) str.append(", ");
                str.append(index.getKeyColumns().get(firstColumn + j).getColumn().getName());
            }
            str.append("):\n");
        }
        str.append(entries);
        return str.toString();
    }

    public int getFirstColumn()
    {
        return firstColumn;
    }

    public int getColumnCount()
    {
        return columnCount;
    }

    public List<HistogramEntry> getEntries() {
        return entries;
    }

    public IndexStatistics getIndexStatistics() {
        return indexStatistics;
    }

    public long totalDistinctCount()
    {
        long total = 0;
        for (HistogramEntry entry : entries) {
            if (entry.getEqualCount() > 0)
                total++;
            total += entry.getDistinctCount();
        }
        return total;
    }

    public Histogram(int firstColumn, int columnCount, List<HistogramEntry> entries)
    {
        this.firstColumn = firstColumn;
        this.columnCount = columnCount;
        this.entries = entries;
    }

    void setIndexStatistics(IndexStatistics indexStatistics) {
        this.indexStatistics = indexStatistics;
    }

    private IndexStatistics indexStatistics;
    private final int firstColumn;
    private final int columnCount;
    private final List<HistogramEntry> entries;
}
