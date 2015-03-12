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
package com.foundationdb.server.test.it.bugs.bug1208930;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.it.qp.OperatorITBase;

import org.junit.Test;

public class PartialCascadeHKeyIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        w = createTable(
            "s", "w",
            "wid INT NOT NULL",
            "PRIMARY KEY(wid)");
        d = createTable(
            "s", "d",
            "wid INT NOT NULL",
            "did INT NOT NULL",
            "PRIMARY KEY(wid, did)",
            "GROUPING FOREIGN KEY(wid) REFERENCES w(wid)");
        c = createTable(
            "s", "c",
            "wid INT NOT NULL", 
            "did INT NOT NULL",
            "cid INT NOT NULL", 
            "PRIMARY KEY(wid, did, cid)", 
            "GROUPING FOREIGN KEY(wid, did) REFERENCES d(wid, did)");
        o = createTable(
            "s", "o",
            "wid INT NOT NULL",
            "did INT NOT NULL",
            "cid INT NOT NULL",
            "oid INT NOT NULL",
            "PRIMARY KEY(wid, did, oid)",
            "GROUPING FOREIGN KEY(wid, did, cid) REFERENCES c(wid, did, cid)");
        i = createTable(
            "s", "i",
            "wid INT NOT NULL",
            "did INT NOT NULL",
            "oid INT NOT NULL",
            "iid INT NOT NULL",
            "PRIMARY KEY(wid, did, iid)",
            "GROUPING FOREIGN KEY(wid, did, oid) REFERENCES o(wid, did, oid)");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = SchemaCache.globalSchema(ais());
        wRowType = schema.tableRowType(table(w));
        dRowType = schema.tableRowType(table(d));
        cRowType = schema.tableRowType(table(c));
        oRowType = schema.tableRowType(table(o));
        iRowType = schema.tableRowType(table(i));
        wOrdinal = ddl().getTable(session(), w).getOrdinal();
        dOrdinal = ddl().getTable(session(), d).getOrdinal();
        cOrdinal = ddl().getTable(session(), c).getOrdinal();
        oOrdinal = ddl().getTable(session(), o).getOrdinal();
        iOrdinal = ddl().getTable(session(), i).getOrdinal();
        group = group(c);
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        loadDatabase();
    }

    private void loadDatabase()
    {
        db = new Row[] {
            row(w, 1L),
            row(d, 1L, 11L),
            row(c, 1L, 11L, 111L),
            row(o, 1L, 11L, 111L, 1111L),
            row(i, 1L, 11L, 1111L, 11111L),
        };
        use(db);
    }

    @Test
    public void testHKeys()
    {
        Operator plan = API.groupScan_Default(group);
        Row[] expected = new Row[] {
            row("{1,(long)1}", wRowType, 1L),
            row("{1,(long)1,2,(long)11}", dRowType, 1L, 11L),
            row("{1,(long)1,2,(long)11,3,(long)111}", cRowType, 1L, 11L, 111L),
            row("{1,(long)1,2,(long)11,3,(long)111,4,(long)1111}", oRowType, 1L, 11L, 111L, 1111L),
            row("{1,(long)1,2,(long)11,3,(long)111,4,(long)1111,5,(long)11111}", iRowType, 1L, 11L, 1111L, 11111L),
        };
        compareRows(expected, API.cursor(plan, queryContext, queryBindings));
    }

    private int w, d, c, o, i;
    private TableRowType wRowType, dRowType, cRowType, oRowType, iRowType;
    private Group group;
    private int wOrdinal, dOrdinal, cOrdinal, oOrdinal, iOrdinal;
}
