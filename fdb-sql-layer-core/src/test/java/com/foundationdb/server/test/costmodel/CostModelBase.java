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
package com.foundationdb.server.test.costmodel;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.test.ApiTestBase;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.util.tap.Tap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.abs;

public class CostModelBase extends ApiTestBase
{
    protected CostModelBase()
    {
        super("CT");
        disableTaps();
    }

    protected Group group(int tableId)
    {
        return ais().getTable(tableId).getGroup();
    }

    protected Table table(int tableId)
    {
        return ais().getTable(tableId);
    }

    protected IndexRowType indexType(int tableId, String... searchIndexColumnNamesArray)
    {
        Table table = table(tableId);
        for (Index index : table.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<>();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                indexColumnNames.add(indexColumn.getColumn().getName());
            }
            List<String> searchIndexColumnNames = Arrays.asList(searchIndexColumnNamesArray);
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return schema.tableRowType(table(tableId)).indexRowType(index);
            }
        }
        return null;
    }

    protected String schemaName()
    {
        return "schema";
    }
    
    protected String newTableName()
    {
        return String.format("t%s", abs(System.nanoTime()));
    }

    private static void disableTaps()
    {
        Tap.setEnabled(".*", false);
    }

    protected Schema schema;
    protected StoreAdapter adapter;
    protected QueryContext queryContext;
    protected QueryBindings queryBindings;
}
