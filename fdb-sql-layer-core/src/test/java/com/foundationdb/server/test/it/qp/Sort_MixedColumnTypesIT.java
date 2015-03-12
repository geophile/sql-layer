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

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowsBuilder;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;

import org.junit.Before;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;

public final class Sort_MixedColumnTypesIT extends ITBase {
    
    @Before
    public void createSchema() {
        customer = createTable(
                "schema", "customer",
                "cid int not null primary key",
                "name varchar(32)",
                "importance decimal(5,2)"
        );
        createIndex(
                "schema", "customer", "importance_and_name",
                "importance", "name"
        );
        // These values have been picked for the following criteria:
        // - all three columns (pk and the two indexed columns) are of different types
        // - neither 'name' nor 'importance' are consistently ordered relative to cid
        // - when the rows are ordered by name, they are unordered by importance
        // - when the rows are ordered by importance, they are unordered by name
        writeRows(
                row(customer, 1L, "Ccc", "100.00"),
                row(customer, 2L, "Aaa", "75.25"),
                row(customer, 3L, "Bbb", "120.00"),
                row(customer, 4L, "Aaa", "32.00")
        );

        schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        Table cTable = getTable(customer);
        customerRowType = schema.tableRowType(cTable);
        customerGroup = cTable.getGroup();

    }

    @Test
    public void unidirectional() {
        Ordering ordering = API.ordering();
        orderBy(ordering, 1, true);
        orderBy(ordering, 2, true);

        Operator plan = sort_General(
                groupScan_Default(customerGroup),
                customerRowType,
                ordering,
                SortOption.PRESERVE_DUPLICATES
        );
        Row[] expected = new RowsBuilder(MNumeric.INT.instance(false),
                             MString.VARCHAR.instance(32, true),
                             MNumeric.DECIMAL.instance(5,2, true)) 
                .row(4, "Aaa", "32.00")
                .row(2, "Aaa", "75.25")
                .row(3, "Bbb", "120.00")
                .row(1, "Ccc", "100.00")
                .rows().toArray(new Row[4]);
        compareRows(expected, cursor(plan));
    }

    @Test
    public void mixed() {
        Ordering ordering = API.ordering();
        orderBy(ordering, 1, true);
        orderBy(ordering, 2, false);

        Operator plan = sort_General(
                groupScan_Default(customerGroup),
                customerRowType,
                ordering,
                SortOption.PRESERVE_DUPLICATES
        );
        Row[] expected = new RowsBuilder(MNumeric.INT.instance(false),
                             MString.VARCHAR.instance(32, true),
                             MNumeric.DECIMAL.instance(5,2, true))
                .row(2, "Aaa", "75.25")
                .row(4, "Aaa", "32.00")
                .row(3, "Bbb", "120.00")
                .row(1, "Ccc", "100.00")
                .rows().toArray(new Row[4]);
        compareRows(expected, cursor(plan));
    }

    private Cursor cursor(Operator plan) {
        StoreAdapter adapter = newStoreAdapter();
        QueryContext context = queryContext(adapter);
        QueryBindings bindings = context.createBindings();
        return API.cursor(plan, context, bindings);
    }

    private void orderBy(Ordering ordering, int fieldPos, boolean ascending) {
        ExpressionGenerator expression = field(customerRowType, fieldPos);
        ordering.append(expression, ascending);
    }

    private Schema schema;
    private int customer;
    private Group customerGroup;
    private TableRowType customerRowType;
}
