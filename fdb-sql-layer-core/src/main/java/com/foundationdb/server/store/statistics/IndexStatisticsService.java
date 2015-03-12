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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.ais.model.Index;

import java.io.Writer;
import java.util.Collection;
import java.io.File;
import java.io.IOException;

public interface IndexStatisticsService
{
    public final static TableName INDEX_STATISTICS_TABLE_NAME = new TableName(TableName.INFORMATION_SCHEMA, "index_statistics");
    public final static TableName INDEX_STATISTICS_ENTRY_TABLE_NAME = new TableName(INDEX_STATISTICS_TABLE_NAME.getSchemaName(), "index_statistics_entry");

    /** Get available statistics for the given index. */
    public IndexStatistics getIndexStatistics(Session session, Index index);

    /** Update statistics for the given indexes. */
    public void updateIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes);

    /** Delete stored statistics for the given indexes. */
    public void deleteIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes);

    /** Load statistics from a YAML file. */
    public void loadIndexStatistics(Session session, 
                                    String schema, File file) throws IOException;

    /** Dump statistics to a YAML file. */
    public void dumpIndexStatistics(Session session, 
                                    String schema, Writer file) throws IOException;

    /** Delete stored statistics for a schema */
    public void deleteIndexStatistics(Session session,
                                      String schema) throws IOException;

    /** Clear the in-memory cache. */
    public void clearCache();

    /** How many buckets to compute per index */
    public int bucketCount();

    /** Note missing statistics: warn user, initiate background analyze. */
    public void missingStats(Session session, Index index, Column column);

    /** Check for out of date stats, based on table being much larger. */
    public void checkRowCountChanged(Session session, Table table,
                                     IndexStatistics stats, long rowCount);
}
