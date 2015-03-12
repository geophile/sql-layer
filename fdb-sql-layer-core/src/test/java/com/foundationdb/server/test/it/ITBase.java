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
package com.foundationdb.server.test.it;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.ApiTestBase;
import com.foundationdb.server.test.it.qp.TestRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class ITBase extends ApiTestBase {
    public ITBase() {
        super("IT");
    }

    protected ITBase(String suffix) {
        super(suffix);
    }

    protected static Row testRow(RowType type, Object... fields) {
        return new TestRow(type, fields);
    }

    protected void compareRows(Object[][] expected, Table table) {
        Schema schema = SchemaCache.globalSchema(ais());
        TableRowType rowType = schema.tableRowType(table);
        Row[] rows = new Row[expected.length];
        for (int i = 0; i < expected.length; i++) {
            rows[i] = new TestRow(rowType, expected[i]);
        }
        
        StoreAdapter adapter = newStoreAdapter();
        QueryContext queryContext = new SimpleQueryContext(adapter);
        
        List<TableRowType> keepTypes = Arrays.asList(rowType);
        
        compareRows (
                rows, 
                API.cursor(API.filter_Default(API.groupScan_Default(table.getGroup()),
                            keepTypes),
                        queryContext, 
                        queryContext.createBindings())
            );
    }
    
    protected void compareRows(Object[][] expected, Index index) {
        Schema schema = SchemaCache.globalSchema(ais());
        IndexRowType rowType = schema.indexRowType(index);
        Row[] rows = new Row[expected.length];
        for(int i = 0; i < expected.length; ++i) {
            rows[i] = new TestRow(rowType, expected[i]);
        }
        StoreAdapter adapter = newStoreAdapter();
        QueryContext queryContext = new SimpleQueryContext(adapter);
        compareRows(
            rows,
            API.cursor(
                API.indexScan_Default(rowType, false, IndexKeyRange.unbounded(rowType)),
                queryContext,
                queryContext.createBindings()
            )
        );
    }

    protected static void compareRows(Row[] expected, Row[] actual) {
        compareRows(Arrays.asList(expected), Arrays.asList(actual));
    }

    protected static void compareRows(Collection<? extends Row> expected, Collection<? extends Row> actual) {
        compareRows(expected, actual, false);
    }

    protected void compareRows(Row[] expected, RowCursor cursor)
    {
        compareRows(expected, cursor, (cursor instanceof Cursor));
    }

    protected void compareRows(Row[] expected, RowCursor cursor, boolean topLevel) {
        boolean began = false;
        if(!txnService().isTransactionActive(session())) {
            txnService().beginTransaction(session());
            began = true;
        }
        boolean success = false;
        try {
            compareRowsInternal(expected, cursor, topLevel);
            success = true;
        } finally {
            if(began) {
                if(success) {
                    txnService().commitTransaction(session());
                } else {
                    txnService().rollbackTransaction(session());
                }
            }
        }
    }

    private void compareRowsInternal(Row[] expected, RowCursor cursor, boolean topLevel)
    {
        List<Row> actualRows = new ArrayList<>(); // So that result is viewable in debugger
        try {
            if (topLevel)
                ((Cursor)cursor).openTopLevel();
            else
                cursor.open();
            Row actualRow;
            while ((actualRow = cursor.next()) != null) {
                int count = actualRows.size();
                assertTrue(String.format("failed test %d < %d (more rows than expected)", count, expected.length), count < expected.length);
                compareTwoRows(expected[count], actualRow, count);
                actualRows.add(actualRow);
            }
        } finally {
            if (topLevel)
                ((Cursor)cursor).closeTopLevel();
            else
                cursor.close();
        }
        assertEquals(expected.length, actualRows.size());
    }

    public void printRows(RowCursor cursor)
    {
        printRows(cursor,
                  new RowFormatter()
                  {
                      @Override
                      public String format(Row row)
                      {
                          return String.valueOf(row);
                      }
                  });
    }

    public void printRows(RowCursor cursor, RowFormatter formatter)
    {
        boolean topLevel = cursor instanceof Cursor;
        boolean began = false;
        if(!txnService().isTransactionActive(session())) {
            txnService().beginTransaction(session());
            began = true;
        }
        try {
            if (topLevel)
                ((Cursor)cursor).openTopLevel();
            else
                cursor.open();
            Row row;
            while ((row = cursor.next()) != null) {
                System.out.println(formatter.format(row));
            }
        } finally {
            if (topLevel)
                ((Cursor)cursor).closeTopLevel();
            else
                cursor.close();
            if(began) {
                txnService().commitTransaction(session());
            }
        }
    }

    public void lookForDanglingStorage() throws Exception {
        // Collect all trees storage currently has
        Set<String> storeTrees = new TreeSet<>();
        storeTrees.addAll(store().getStorageDescriptionNames(session()));

        // Collect all trees in AIS
        Set<String> smTrees = serviceManager().getSchemaManager().getTreeNames(session());

        // Subtract knownTrees from storage trees instead of requiring exact. There may be allocated trees that
        // weren't materialized (yet), for example.
        Set<String> difference = new TreeSet<>(storeTrees);
        difference.removeAll(smTrees);

        assertEquals("Found orphaned trees", "[]", difference.toString());
    }

    public interface RowFormatter
    {
        String format(Row row);
    }
}

