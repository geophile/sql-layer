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

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.test.ExpressionGenerators;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;

// Inspired by Bug 979162

public class IndexScanInvolvingUndeclaredColumnsIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        region = createTable(
            "schema", "region",
            "rid int not null",
            "primary key(rid)");
        regionChildren = createTable(
            "schema", "region_children",
            "rid int not null",
            "locid int not null",
            "grouping foreign key(rid) references region(rid)");
        createIndex("schema", "region_children", "idx_locid", "locid");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        regionChildrenRowType = schema.tableRowType(table(regionChildren));
        idxRowType = indexType(regionChildren, "locid");
        db = new Row[]{
            // region
            row(region, 10L),
            row(region, 20L),
            // region_children (last column is hidden PK)
            row(regionChildren, 10L, 100L, 1L),
            row(regionChildren, 10L, 110L, 2L),
            row(regionChildren, 10L, 120L, 3L),
            row(regionChildren, 20L, 200L, 4L),
            row(regionChildren, 20L, 210L, 5L),
            row(regionChildren, 20L, 220L, 6L),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    @Test
    public void test()
    {
        IndexBound bound = new IndexBound(row(idxRowType, 110L, 15L),
                                          new SetColumnSelector(0, 1));
        IndexKeyRange range = IndexKeyRange.bounded(idxRowType, bound, true, bound, true);
        API.Ordering ordering = new API.Ordering();
        ordering.append(ExpressionGenerators.field(idxRowType, 0), true);
        ordering.append(ExpressionGenerators.field(idxRowType, 1), true);
        Operator plan =
            indexScan_Default(
                idxRowType,
                range,
                ordering);
        compareRows(new Row[0], cursor(plan, queryContext, queryBindings));
    }

    // For use by this class

    private API.Ordering ordering(Object ... ord) // alternating column positions and asc/desc
    {
        API.Ordering ordering = API.ordering();
        int i = 0;
        while (i < ord.length) {
            int column = (Integer) ord[i++];
            boolean asc = (Boolean) ord[i++];
            ordering.append(field(idxRowType, column), asc);
        }
        return ordering;
    }

    private int region;
    private int regionChildren;
    private RowType regionChildrenRowType;
    private IndexRowType idxRowType;
}
