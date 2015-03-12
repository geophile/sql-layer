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
package com.foundationdb.server.test.it.keyupdate;

import java.util.List;

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.value.Value;

public class KeyUpdateRow extends ValuesHolderRow
{
    public KeyUpdateRow(RowType type, Store store, Object...objects) 
    {
        super (type, objects);
        this.store = store;
    }
    
    public KeyUpdateRow(RowType type, Store store, List<Value> values) {
        super (type, values);
        this.store = store;
    }

    public com.foundationdb.server.test.it.keyupdate.HKey keyUpdateHKey()
    {
        return hKey;
    }

    public void hKey(HKey hKey)
    {
        this.hKey = hKey;
    }

    public KeyUpdateRow parent()
    {
        return parent;
    }

    public void parent(KeyUpdateRow parent)
    {
        this.parent = parent;
    }

    public Store getStore() {
        return store;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        
        if (obj instanceof Row) {
            return this.compareTo((Row)obj, 0, 0, rowType().nFields()) == 0;
        }
        return false;
    }

    private com.foundationdb.server.test.it.keyupdate.HKey hKey;
    private KeyUpdateRow parent;
    private final Store store;
}
