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
package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.Store;
import com.foundationdb.util.tap.InOutTap;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public abstract class StoreAdapter
{
    public abstract GroupCursor newGroupCursor(Group group);

    public static final int COMMIT_FREQUENCY_PERIODICALLY = -2;

    public GroupCursor newDumpGroupCursor(Group group, int commitFrequency) {
        return newGroupCursor(group);
    }

    public abstract RowCursor newIndexCursor(QueryContext context,
                                             IndexRowType rowType,
                                             IndexKeyRange keyRange, 
                                             API.Ordering ordering,
                                             IndexScanSelector scanSelector,
                                             boolean openAllSubCursors);
    
    public abstract void updateRow(Row oldRow, Row newRow);

    public void writeRow(Row newRow) {
        writeRow(newRow, null, null);
    }

    public abstract void writeRow(Row newRow, Collection<TableIndex> tableIndexes, Collection<GroupIndex> groupIndexes);
    
    public abstract void deleteRow (Row oldRow, boolean cascadeDelete);

    public abstract Sorter createSorter(QueryContext context,
                                        QueryBindings bindings,
                                        RowCursor input,
                                        RowType rowType,
                                        API.Ordering ordering,
                                        API.SortOption sortOption,
                                        InOutTap loadTap);

    public long getQueryTimeoutMilli() {
        return config.queryTimeoutMilli();
    }

    public long rowCount(Session session, RowType tableType) {
        assert tableType.hasTable() : tableType;
        return tableType.table().tableStatus().getRowCount(session);
    }

    public Sequence getSequence(TableName sequenceName) {
        Sequence sequence = getAIS().getSequence(sequenceName);
        if(sequence == null) {
            throw new NoSuchSequenceException(sequenceName);
        }
        return sequence;
    }

    public abstract long sequenceNextValue(Sequence sequence);

    public abstract long sequenceCurrentValue(Sequence sequence);

    public final Session getSession() {
        return session;
    }

    public abstract IndexRow newIndexRow (IndexRowType indexRowType);
    
    public abstract IndexRow takeIndexRow(IndexRowType indexRowType);

    public abstract void returnIndexRow(IndexRow indexRow);

    public abstract IterationHelper createIterationHelper(IndexRowType indexRowType);

    public long id() {
        return id;
    }

    public final ConfigurationService getConfig() {
        return config;
    }
    
    public abstract KeyCreator getKeyCreator();

    protected abstract Store getUnderlyingStore();
    
    public abstract AkibanInformationSchema getAIS();

    protected StoreAdapter(Session session,
            ConfigurationService config)
    {
        this.session = session;
        this.config = config;
    }

    // Class state

    private static final AtomicLong idCounter = new AtomicLong(0);

    // Object state

    private final Session session;
    private final ConfigurationService config;
    private final long id = idCounter.incrementAndGet();
}
