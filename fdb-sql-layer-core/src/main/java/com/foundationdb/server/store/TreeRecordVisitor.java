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

import com.foundationdb.ais.model.HKey;
import com.foundationdb.ais.model.HKeySegment;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.session.Session;
import com.persistit.Key;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TreeRecordVisitor
{
    public final void initialize(Session session, Store store)
    {
        this.session = session;
        this.store = store;
        for (Table table : store.getAIS(session).getTables().values()) {
            if (!table.getName().getSchemaName().equals(TableName.INFORMATION_SCHEMA) &&
                !table.getName().getSchemaName().equals(TableName.SECURITY_SCHEMA)) {
                ordinalToTable.put(table.getOrdinal(), table);
            }
        }
    }


    public void visit(Key key, Row row)  {
        Object[] keyObjs = key(key, row);
        visit(keyObjs, row);
    }
    
    public abstract void visit(Object[] key, Row row);
    
    
    private Object[] key(Key key, Row row) {
        // Key traversal
        int keySize = key.getDepth();
        // HKey traversal
        HKey hKey = row.rowType().table().hKey();
        List<HKeySegment> hKeySegments = hKey.segments();
        int k = 0;
        // Traverse key, guided by hKey, populating result
        Object[] keyArray = new Object[keySize];
        int h = 0;
        key.indexTo(0);
        while (k < hKeySegments.size()) {
            HKeySegment hKeySegment = hKeySegments.get(k++);
            Table table = hKeySegment.table();
            int ordinal = (Integer) key.decode();
            assert ordinalToTable.get(ordinal) == table : ordinalToTable.get(ordinal);
            keyArray[h++] = table;
            for (int i = 0; i < hKeySegment.columns().size(); i++) {
                keyArray[h++] = key.decode();
            }
        }
        return keyArray;
        
    }

    private Store store;
    private Session session;
    private final Map<Integer, Table> ordinalToTable = new HashMap<>();
}
