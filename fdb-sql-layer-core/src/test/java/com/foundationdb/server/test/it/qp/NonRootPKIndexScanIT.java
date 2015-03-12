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
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.test.ExpressionGenerators;

import org.junit.Ignore;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;

// Inspired by bug 1033754.

public class NonRootPKIndexScanIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        parent = createTable(
            "schema", "parent",
            "pid1 int not null",
            "pid2 int not null",
            "primary key(pid1, pid2)");
        child = createTable(
            "schema", "child",
            "cid int not null",
            "pid1 int",
            "pid2 int",
            "primary key(cid)",
            "grouping foreign key (pid1, pid2) references parent(pid1, pid2)");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        childPKRowType = indexType(child, "cid");
        db = new Row[] {
            row(parent, 1L, 1L),
            row(child, 11L, 1L, 1L),
            row(child, 12L, 1L, 1L),
            row(parent, 2L, 2L),
            row(child, 21L, 2L, 2L),
            row(child, 22L, 2L, 2L),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    @Test
    public void testAtChildAtParentUnidirectional()
    {
        long[] childPKs = new long[] {11, 12, 21, 22};
        int[] orderingMasks = new int[]{ALL_ASCENDING, ALL_DESCENDING};
        for (int orderingMask : orderingMasks) {
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
        }
    }

    @Test
    public void testAtChildAfterParentUnidirectional()
    {
        long[] childPKs = new long[] {11, 12, 21, 22};
        int[] orderingMasks = new int[]{ALL_ASCENDING, ALL_DESCENDING};
        for (int orderingMask : orderingMasks) {
            // Empty due to pid1
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK + 1, parentPK + 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK + 1, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
            // Empty due to pid2
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
            // Non-empty
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
        }
    }

    @Test
    public void testAtChildBeforeParentUnidirectional()
    {
        long[] childPKs = new long[] {11, 12, 21, 22};
        int[] orderingMasks = new int[]{ALL_ASCENDING, ALL_DESCENDING};
        for (int orderingMask : orderingMasks) {
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK - 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
        }
    }

    private API.Ordering ordering(int mask)
    {
        API.Ordering ordering = new API.Ordering();
        ordering.append(ExpressionGenerators.field(childPKRowType, 0), (mask & 0x1) != 0);
        ordering.append(ExpressionGenerators.field(childPKRowType, 1), (mask & 0x2) != 0);
        ordering.append(ExpressionGenerators.field(childPKRowType, 2), (mask & 0x4) != 0);
        return ordering;
    }

    private static final int ALL_ASCENDING = 0x7;
    private static final int ALL_DESCENDING = 0x0;
    private static final ColumnSelector SELECTOR = new SetColumnSelector(0, 1, 2);

    private int parent;
    private int child;
    private RowType parentRowType;
    private RowType childRowType;
    private IndexRowType childPKRowType;
}
