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
package com.foundationdb.server.test.it.qp;

import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.*;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.it.ITBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class GroupIndexScanIT extends ITBase {
    @Test
    public void scanAtLeastO () {
        Operator plan = API.indexScan_Default(giRowType, false, unboundedRange(giRowType), tableRowType(o));
        compareResults(plan,
                array("01-01-2001", null),
                array("02-02-2002", "1111"),
                array("03-03-2003", null),
                array("03-03-2003", "3333")
        );
    }

    @Test
    public void scanAtLeastI () {
        Operator plan = API.indexScan_Default(giRowType, false, unboundedRange(giRowType), tableRowType(i));
        compareResults(plan,
                array("02-02-2002", "1111"),
                array("03-03-2003", null),
                array("03-03-2003", "3333")
        );
    }

    @Test
    public void defaultDepth() {
        Operator explicit = API.indexScan_Default(giRowType, false, unboundedRange(giRowType), tableRowType(i));
        Operator defaulted = API.indexScan_Default(giRowType, false, unboundedRange(giRowType));

        List<List<?>> explicitList = planToList(explicit);
        List<List<?>> defaultedList = planToList(defaulted);

        assertEqualLists("results", explicitList, defaultedList);
    }

    @Test(expected = IllegalArgumentException.class)
    public void scanAtLeastC () {
        API.indexScan_Default(giRowType, false, null, tableRowType(c));
    }

    @Test(expected = IllegalArgumentException.class)
    public void scanAtLeastH () {
        API.indexScan_Default(giRowType, false, null, tableRowType(h));
    }

    @Before
    public void setUp() {
        c = createTable(SCHEMA, "c", "cid int not null primary key, name varchar(32)");
        o = createTable(SCHEMA, "o", "oid int not null primary key, c_id int", "when varchar(32)", akibanFK("c_id", "c", "cid"));
        i = createTable(SCHEMA, "i", "iid int not null primary key, o_id int", "sku varchar(6)", akibanFK("o_id", "o", "oid"));
        h = createTable(SCHEMA, "h", "hid int not null primary key, i_id int", akibanFK("i_id", "i", "iid"));
        TableName groupName = getTable(c).getGroup().getName();
        GroupIndex gi = createLeftGroupIndex(groupName, GI_NAME, "o.when", "i.sku");

        schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        giRowType = schema.indexRowType(gi);

        writeRows(
                row(c, 1L, "One"),
                row(o, 10L, 1L, "01-01-2001"), // no children
                row(o, 11L, 1L, "02-02-2002"), // one child
                row(i, 100L, 11L, "1111"),
                row(o, 12L, 2L, "03-03-2003"), // orphaned, two children
                row(i, 101L, 12L, null),
                row(i, 102L, 12L, "3333")
        );

        txnService().beginTransaction(session());
    }

    @After
    public void tearDown() {
        txnService().rollbackTransaction(session());
        c = o = i = h = null;
        schema = null;
        giRowType = null;
        adapter = null;
    }

    private TableRowType tableRowType(int tableId) {
        Table table = ddl().getAIS(session()).getTable(tableId);
        TableRowType rowType = schema.tableRowType(table);
        if (rowType == null) {
            throw new NullPointerException(table.toString());
        }
        return rowType;
    }

    private void compareResults(Operator plan, Object[]... expectedResults) {
        assertEqualLists("rows scanned", nestedList(expectedResults), planToList(plan));
    }

    private List<List<?>> planToList(Operator plan) {
        List<List<?>> actualResults = new ArrayList<>();
        Cursor cursor =  API.cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        try {
            for (Row row = cursor.next(); row != null; row = cursor.next()) {
                RowType rowType = row.rowType();
                int fields =
                    rowType instanceof IndexRowType
                    ? ((IndexRowType)rowType).index().getKeyColumns().size()
                    : rowType.nFields();
                Object[] rowArray = new Object[fields];
                for (int i=0; i < rowArray.length; ++i) {
                    Object fromRow;
                    fromRow = getObject(row.value(i));
                    rowArray[i] = fromRow;
                }
                actualResults.add(Arrays.asList(rowArray));
            }
        } finally {
            cursor.closeTopLevel();
        }
        return actualResults;
    }

    private List<List<?>> nestedList(Object[][] input) {
        List<List<?>> listList = new ArrayList<>();
        for (Object[] array : input) {
            listList.add(Arrays.asList(array));
        }
        return listList;
    }

    private IndexKeyRange unboundedRange(IndexRowType indexRowType)
    {
        return IndexKeyRange.unbounded(indexRowType);
    }

    private Integer c, o, i, h;
    private Schema schema;
    private StoreAdapter adapter;
    private QueryContext queryContext;
    private QueryBindings queryBindings;
    private IndexRowType giRowType;

    private final static String SCHEMA = "schema";
    private final static String GI_NAME = "when_sku";
}
