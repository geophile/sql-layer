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
package com.foundationdb.qp.row;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexRowComposition;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.storeadapter.indexcursor.SortKeyAdapter;
import com.foundationdb.qp.storeadapter.indexcursor.SortKeyTarget;
import com.foundationdb.qp.storeadapter.indexcursor.ValueSortKeyAdapter;
import com.foundationdb.qp.storeadapter.indexrow.SpatialColumnHandler;
import com.foundationdb.qp.util.PersistitKey;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.value.ValueSource;
import com.persistit.Key;
import com.persistit.Value;

public class WriteIndexRow extends AbstractRow {

    @Override
    public RowType rowType() {
        return indexRowType;
    }

    public void initialize (Row row, Key hKey, SpatialColumnHandler spatialColumnHandler, long zValue) {
        pKeyAppends = 0;
        int indexField = 0;
        IndexRowComposition indexRowComp = index.indexRowComposition();
        while (indexField < indexRowComp.getLength()) {
            // handleSpatialColumn will increment pKeyAppends once for all spatial columns
            if (spatialColumnHandler != null && spatialColumnHandler.handleSpatialColumn(this, indexField, zValue)) {
                if (indexField == index.firstSpatialArgument()) {
                    pKeyAppends++;
                }
            } else {
                if (indexRowComp.isInRow(indexField)) {
                    int position = indexRowComp.getFieldPosition(indexField);
                    Column column = row.rowType().table().getColumnsIncludingInternal().get(position);
                    ValueSource source = row.value(column.getPosition());
                    pKeyTarget().append(source, column.getType());
                } else if (indexRowComp.isInHKey(indexField)) {
                    PersistitKey.appendFieldFromKey(pKey(), hKey, indexRowComp.getHKeyPosition(indexField), index
                        .getIndexName());
                } else {
                    throw new IllegalStateException("Invalid IndexRowComposition: " + indexRowComp);
                }
                pKeyAppends++;
            }
            indexField++;
        }
        
    }

    public void close(Session session, boolean forInsert) {
        //If we've written too many fields to the key (iKey), make sure to put the extra fields
        // into the value (iValue) for writing. 
        if (pValueTarget != null) {
            iValue.clear();
            iValue.putByteArray(iKeyExtended.getEncodedBytes(), 0, iKeyExtended.getEncodedSize());
        }
    }

    public Key pKey()
    {
        if (pKeyAppends < pKeyFields) {
            return iKey;
        }
        return iKeyExtended;
    }

    @SuppressWarnings("unchecked")
    private <S> SortKeyTarget<S> pKeyTarget()
    {
        if (pKeyAppends < pKeyFields) {
            return pKeyTarget;
        }
        return pValueTarget;
    }

    public void resetForWrite(Index index, Key createKey) {
        resetForWrite(index, createKey, null);
    }
    
    public void resetForWrite(Index index, Key createKey, Value value) {
        this.index = index;
        this.iKey = createKey;
        this.iValue = value;
        this.index = index;
        if (index.isSpatial()) {
            this.pKeyFields = index.getAllColumns().size() - index.spatialColumns() + 1;
        } else {
            this.pKeyFields = index.getAllColumns().size();
        }
        if (this.pKeyTarget == null) {
            this.pKeyTarget = SORT_KEY_ADAPTER.createTarget(index.getIndexName());
        }
        this.pKeyTarget.attach(createKey);
    }

    // Group Index Row only - table bitmap stored in index value
    public void tableBitmap(long bitmap) {
        iValue.put(bitmap);
    }

    public long tableBitmap() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public <S> void append(S source, TInstance type) {
        pKeyTarget.append(source, type);
    }

    public WriteIndexRow () {
    }
    
    private Index index;
    private Key iKey;
    private Key iKeyExtended;
    private Value iValue;
    private IndexRowType indexRowType;
    private SortKeyTarget pKeyTarget;
    private SortKeyTarget pValueTarget;
    private int pKeyAppends = 0;
    private int pKeyFields;

    private final SortKeyAdapter<ValueSource, TPreparedExpression> SORT_KEY_ADAPTER = ValueSortKeyAdapter.INSTANCE;
    
    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    protected ValueSource uncheckedValue(int i) {
        throw new UnsupportedOperationException();
    }

}
