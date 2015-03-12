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
package com.foundationdb.server.store;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.IndexRowComposition;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.WriteIndexRow;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.spatial.Spatial;
import com.foundationdb.util.ArgumentValidation;
import com.geophile.z.Space;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StoreGIHandler<SType extends AbstractStore,SDType,SSDType extends StoreStorageDescription<SType,SDType>> {
    private static final TInstance NON_NULL_Z_TYPE = InternalIndexTypes.LONG.instance(false);
    private static final Logger LOG = LoggerFactory.getLogger(StoreGIHandler.class);

    public static enum GroupIndexPosition {
        ABOVE_SEGMENT,
        BELOW_SEGMENT,
        WITHIN_SEGMENT
    }

    public static enum Action {
        STORE,
        DELETE,
        CASCADE,
        CASCADE_STORE
    }

    private final AbstractStore<SType,SDType,SSDType> store;
    private final Session session;
    private final Table sourceTable;
    private final WriteIndexRow indexRow;
    private final Value zSource_t3 = new Value(InternalIndexTypes.LONG.instance(true));
    private final Collection<RowType> lockTypes;

    private StoreGIHandler(AbstractStore<SType,SDType,SSDType> store, Session session, Schema schema, Table sourceTable, Table lockLeaf) {
        this.store = store;
        this.session = session;
        this.indexRow = new WriteIndexRow();
        this.sourceTable = sourceTable;
        if(lockLeaf == null) {
            this.lockTypes = null;
        } else {
            this.lockTypes = new ArrayList<>(lockLeaf.getDepth());
            for(Table table = lockLeaf; table != null; table = table.getParentTable()) {
                lockTypes.add(schema.tableRowType(table));
            }
        }
    }

    public static <SType extends AbstractStore,SDType,SSDType extends StoreStorageDescription<SType,SDType>> StoreGIHandler forTable(AbstractStore<SType,SDType,SSDType> store,
                                                                                                               Session session,
                                                                                                               Table table) {
        ArgumentValidation.notNull("table", table);
        return new StoreGIHandler<>(store, session, null, table, null);
    }

    public static <SType extends AbstractStore,SDType,SSDType extends StoreStorageDescription<SType,SDType>> StoreGIHandler forBuilding(AbstractStore<SType,SDType,SSDType> store,
                                                                                                                  Session session,
                                                                                                                  Schema schema,
                                                                                                                  GroupIndex groupIndex) {
        return new StoreGIHandler<>(store, session, schema, null, groupIndex.leafMostTable());
    }

    public void handleRow(GroupIndex groupIndex, Row row, Action action) {
        GroupIndexPosition sourceRowPosition = positionWithinBranch(groupIndex, sourceTable);
        if (sourceRowPosition.equals(GroupIndexPosition.BELOW_SEGMENT)) {
            return; // nothing to do
        }

        if(lockTypes != null) {
            for(RowType type : lockTypes) {
                Row subRow = row.subRow(type);
                if(subRow != null) {
                   store.lock(session, subRow);
                }
            }
        }
        int firstSpatialColumn = groupIndex.isSpatial() ? groupIndex.firstSpatialArgument() : -1;
        SDType storeData = store.createStoreData(session, groupIndex);
        try {
            store.resetForWrite(storeData, groupIndex, indexRow);
            IndexRowComposition irc = groupIndex.indexRowComposition();
            int nFields = irc.getLength();
            int f = 0;
            while(f < nFields) {
                assert irc.isInRow(f);
                assert ! irc.isInHKey(f);
                if(f == firstSpatialColumn) {
                    copyZValueToIndexRow(groupIndex, row, irc);
                    f += groupIndex.spatialColumns();
                } else {
                    copyFieldToIndexRow(groupIndex, row, irc.getFieldPosition(f++));
                }
            }
            // Non-null values only required for UNIQUE indexes, which GIs cannot be
            assert !groupIndex.isUnique() : "Unexpected unique index: " + groupIndex;
            indexRow.close(null, false);
            indexRow.tableBitmap(tableBitmap(groupIndex, row));
            switch (action) {
                case CASCADE_STORE:
                case STORE:
                    store.store(session, storeData);
                break;
                case CASCADE:
                case DELETE:
                    store.clear(session, storeData);
                break;
                default:
                    throw new UnsupportedOperationException(action.name());
            }
        } finally {
            store.releaseStoreData(session, storeData);
        }
    }

    public Table getSourceTable () {
        return sourceTable;
    }

    private void copyFieldToIndexRow(GroupIndex groupIndex, Row row, int flattenedIndex) {
        Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);
        assert row.rowType().nFields() >= flattenedIndex : "Row: " + row.rowType().toString() + " : does not hold all fields for group index " + flattenedIndex;
        indexRow.append(row.value(flattenedIndex), column.getType());
    }

    private void copyZValueToIndexRow(GroupIndex groupIndex, Row row, IndexRowComposition irc) {
        BigDecimal[] coords = new BigDecimal[Spatial.LAT_LON_DIMENSIONS];
        Space space = groupIndex.space();
        int firstSpatialColumn = groupIndex.firstSpatialArgument();
        boolean zNull = false;
        for(int d = 0; d < Spatial.LAT_LON_DIMENSIONS; d++) {
            if(!zNull) {
                ValueSource columnValue = row.value(irc.getFieldPosition(firstSpatialColumn + d));
                if (columnValue.isNull()) {
                    zNull = true;
                } else {
                    coords[d] = ((BigDecimalWrapper)columnValue.getObject()).asBigDecimal();
                }
            }
        }
        if (zNull) {
            zSource_t3.putNull();
            indexRow.append(zSource_t3, NON_NULL_Z_TYPE);
        } else {
            zSource_t3.putInt64(Spatial.shuffle(space, coords[0].doubleValue(), coords[1].doubleValue()));
            indexRow.append(zSource_t3, NON_NULL_Z_TYPE);
        }
    }

    // The group index row's value contains a bitmap indicating which of the tables covered by the index
    // have rows contributing to this index row. The leafmost table of the index is represented by bit
    // position 0.
    private long tableBitmap(GroupIndex groupIndex, Row row) {
        long result = 0;
        Table table = groupIndex.leafMostTable();
        Table end = groupIndex.rootMostTable().getParentTable();
        while(table != null && !table.equals(end)) {
            if(row.containsRealRowOf(table)) {
                result |= (1 << table.getDepth());
            }
            table = table.getParentTable();
        }
        return result;
    }

    private GroupIndexPosition positionWithinBranch(GroupIndex groupIndex, Table table) {
        final Table leafMost = groupIndex.leafMostTable();
        if(table == null) {
            return GroupIndexPosition.ABOVE_SEGMENT;
        }
        if(table.equals(leafMost)) {
            return GroupIndexPosition.WITHIN_SEGMENT;
        }
        if(table.isDescendantOf(leafMost)) {
            return GroupIndexPosition.BELOW_SEGMENT;
        }
        if(groupIndex.rootMostTable().equals(table)) {
            return GroupIndexPosition.WITHIN_SEGMENT;
        }
        return groupIndex.rootMostTable().isDescendantOf(table)
                ? GroupIndexPosition.ABOVE_SEGMENT
                : GroupIndexPosition.WITHIN_SEGMENT;
    }
}
