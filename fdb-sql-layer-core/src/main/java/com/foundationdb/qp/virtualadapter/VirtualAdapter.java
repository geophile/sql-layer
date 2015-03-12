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
package com.foundationdb.qp.virtualadapter;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.API.SortOption;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.format.VirtualTableStorageDescription;
import com.foundationdb.util.tap.InOutTap;

import java.util.Collection;

public class VirtualAdapter extends StoreAdapter {

    public VirtualAdapter(Session session,
                ConfigurationService config) {
        super (session, config);
    }

    public static VirtualScanFactory getFactory(Table table) {
        // NOTE: This assumes that a virtual table group never has more
        // than one table or at least that they all have equivalent
        // factories.
        return getFactory(table.getGroup());
    }

    public static VirtualScanFactory getFactory(Group group) {
        return ((VirtualTableStorageDescription)group.getStorageDescription())
            .getVirtualScanFactory();
    }

    @Override
    public GroupCursor newGroupCursor(Group group) {
        return new VirtualGroupCursor(this, group);
    }


    @Override
    public AkibanInformationSchema getAIS() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    protected Store getUnderlyingStore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowCursor newIndexCursor(QueryContext context, IndexRowType indexType,
            IndexKeyRange keyRange, Ordering ordering,
            IndexScanSelector scanSelector, boolean openAllSubCursors) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Sorter createSorter(QueryContext context, QueryBindings bindings, RowCursor input, RowType rowType,
                               Ordering ordering, SortOption sortOption, InOutTap loadTap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRow(Row oldRow, Row newRow) {
        throw new UnsupportedOperationException();
        
    }

    @Override
    public void writeRow(Row newRow, Collection<TableIndex> indexes, Collection<GroupIndex> groupIndexes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRow(Row oldRow, boolean cascadeDelete) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sequenceNextValue(Sequence sequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sequenceCurrentValue(Sequence sequence) {
        throw new UnsupportedOperationException();
    }

    @Override 
    public IndexRow newIndexRow (IndexRowType indexRowType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexRow takeIndexRow(IndexRowType indexRowType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void returnIndexRow(IndexRow indexRow) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IterationHelper createIterationHelper(IndexRowType indexRowType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyCreator getKeyCreator() {
        throw new UnsupportedOperationException();
    }
}
