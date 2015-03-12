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
package com.foundationdb.server.store.format.tuple;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.server.test.it.qp.TestRow;

import org.junit.Test;

public class TupleStorageFormatIT  extends FDBITBase
{
    private static final String SCHEMA = "test";

    @Test
    public void decimalTypeAllowed() {
        createFromDDL(SCHEMA,
          "CREATE TABLE t1(id INT PRIMARY KEY NOT NULL, d DECIMAL(6,2));" +
          "CREATE INDEX i1 ON t1(d) STORAGE_FORMAT tuple;");
    }

    @Test
    public void groupAllowed() {
        createFromDDL(SCHEMA,
          "CREATE TABLE parent(id INT PRIMARY KEY NOT NULL, s VARCHAR(128)) STORAGE_FORMAT tuple;" +
          "CREATE TABLE child(id INT PRIMARY KEY NOT NULL, pid INT, GROUPING FOREIGN KEY(pid) REFERENCES parent(id))");
    }

    @Test
    public void keyAndRow() {
        createFromDDL(SCHEMA,
          "CREATE TABLE t1(id INT PRIMARY KEY NOT NULL, s VARCHAR(128)) STORAGE_FORMAT tuple");
        int t1 = ddl().getTableId(session(), new TableName(SCHEMA, "t1"));

        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType t1Type = schema.tableRowType(getTable(t1));
        StoreAdapter adapter = newStoreAdapter();

        txnService().beginTransaction(session());

        Object[] r1 = { 1L, "Fred" };
        Object[] r2 = { 2L, "Barney" };
        writeRow(t1, r1);
        writeRow(t1, r2);

        Row[] expected = {
            new TestRow(t1Type, r1),
            new TestRow(t1Type, r2)
        };
        compareRows(expected, adapter.newGroupCursor(t1Type.table().getGroup()));

        txnService().commitTransaction(session());
    }
    
    @Test(expected=StorageDescriptionInvalidException.class)
    public void keyOnly() {
        createFromDDL(SCHEMA,
          "CREATE TABLE parent(id INT PRIMARY KEY NOT NULL, s VARCHAR(128)) STORAGE_FORMAT tuple(key_only = true);");
    }

}
