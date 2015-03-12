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
package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.API.SortOption;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter;
import com.foundationdb.qp.storeadapter.indexrow.IndexRowPool;
import com.foundationdb.qp.storeadapter.indexrow.MemoryIndexRow;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.MemoryStore;
import com.foundationdb.util.tap.InOutTap;

import java.util.Collection;

public class MemoryAdapter extends StoreAdapter
{
    private static final IndexRowPool indexRowPool = new IndexRowPool();

    private final MemoryStore store;

    public MemoryAdapter(Session session, ConfigurationService config, MemoryStore store) {
        super(session, config);
        this.store = store;
    }


    @Override
    public GroupCursor newGroupCursor(Group group) {
        return new MemoryGroupCursor(this, group);
    }

    @Override
    public RowCursor newIndexCursor(QueryContext context,
                                    IndexRowType rowType,
                                    IndexKeyRange keyRange,
                                    Ordering ordering,
                                    IndexScanSelector scanSelector,
                                    boolean openAllSubCursors) {
        return new StoreAdapterIndexCursor(context,
                                        rowType,
                                        keyRange,
                                        ordering,
                                        scanSelector,
                                        openAllSubCursors);
    }

    @Override
    public void updateRow(Row oldRow, Row newRow) {
        try {
            store.updateRow(getSession(), oldRow, newRow);
        } catch(DuplicateKeyException e) {
            store.setRollbackPending(getSession());
            throw e;
        }
    }

    @Override
    public void writeRow(Row newRow, Collection<TableIndex> tableIndexes, Collection<GroupIndex> groupIndexes) {
        try {
            store.writeRow(getSession(), newRow, tableIndexes, groupIndexes);
        } catch(DuplicateKeyException e) {
            store.setRollbackPending(getSession());
            throw e;
        }
    }

    @Override
    public void deleteRow(Row oldRow, boolean cascadeDelete) {
        try {
            store.deleteRow(getSession(), oldRow, cascadeDelete);
        } catch(DuplicateKeyException e) {
            store.setRollbackPending(getSession());
            throw e;
        }
    }

    @Override
    public Sorter createSorter(QueryContext context,
                               QueryBindings bindings,
                               RowCursor input,
                               RowType rowType,
                               Ordering ordering,
                               SortOption sortOption,
                               InOutTap loadTap) {
        return new MergeJoinSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
    }

    @Override
    public long sequenceNextValue(Sequence sequence) {
        return store.nextSequenceValue(getSession(), sequence);
    }

    @Override
    public long sequenceCurrentValue(Sequence sequence) {
        return store.curSequenceValue(getSession(), sequence);
    }

    @Override
    public IndexRow newIndexRow(IndexRowType indexRowType) {
        return new MemoryIndexRow(getUnderlyingStore(), indexRowType);
    }

    @Override
    public IndexRow takeIndexRow(IndexRowType indexRowType) {
        return indexRowPool.takeIndexRow(this, indexRowType);
    }

    @Override
    public void returnIndexRow(IndexRow indexRow) {
        indexRowPool.returnIndexRow(this, indexRow.rowType(), indexRow);
    }

    @Override
    public IterationHelper createIterationHelper(IndexRowType indexRowType) {
        return new MemoryIterationHelper(this, indexRowType);
    }

    @Override
    public KeyCreator getKeyCreator() {
        return store;
    }

    @Override
    protected MemoryStore getUnderlyingStore() {
        return store;
    }

    @Override
    public AkibanInformationSchema getAIS() {
        return store.getAIS(getSession());
    }
}
