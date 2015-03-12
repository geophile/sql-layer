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

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.types.value.ValueSource;
import com.persistit.Key;

public class FDBGroupRow extends AbstractRow {
    private final KeyCreator keyCreator;
    private final Row underlying;
    private HKey currentHKey;


    public FDBGroupRow(KeyCreator keyCreator, Row abstractRow, Key key) {
        underlying = abstractRow;
        this.keyCreator = keyCreator;
        currentHKey = keyCreator.newHKey(rowType().table().hKey());
        if(key != null) {
            currentHKey.copyFrom(key);
        }
    }

    @Override
    public HKey hKey()
    {
        return currentHKey;
    }

    @Override
    public HKey ancestorHKey(Table table)
    {
        HKey ancestorHKey = keyCreator.newHKey(table.hKey());
        currentHKey.copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }

    @Override
    public boolean containsRealRowOf(Table table)
    {
        return rowType().table() == table;
    }

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }


    @Override
    public RowType rowType() {
        return underlying.rowType();
    }


    @Override
    protected ValueSource uncheckedValue(int i) {
        return underlying.value(i);
    }
}
