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
import org.junit.Ignore;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 * This test covers index scans bound with null on one or both sides
 * lo > null                   lo is unbounded, only valid in last part of IndexKeyRange
 * lo >= null                  lo is unbounded or null, only valid in last part of IndexKeyRange unless hi <= null.
 * hi < null                   hi is unbounded, only valid in last part of IndexKeyRange
 * hi <= null                  invalid
 * lo >= null and hi <= null   IS NULL comparison
 */

public class IndexScanNullIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null primary key",
            "a int",
            "b int");
        createIndex("schema", "t", "a", "a", "b", "id");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "id");
        db = new Row[]{
            // No nulls
            row(t, 1000L, null, null),
            row(t, 1001L, null, 8L),
            row(t, 1002L, 5L, null),
            row(t, 1003L, 5L, 8L),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    @Ignore
    @Test
    public void testGTNull()
    {
        test(range(EXCLUSIVE, null, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED),
             ordering(ASC),
             1002, 1003);
        test(range(EXCLUSIVE, null, null,
                   INCLUSIVE, null, 8),
             ordering(ASC, ASC),
             1001);
        test(range(EXCLUSIVE, null, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED),
             ordering(DESC),
             1003, 1002);
        test(range(EXCLUSIVE, null, null,
                   INCLUSIVE, null, 8),
             ordering(DESC, DESC),
             1001);
        test(range(EXCLUSIVE, null, null,
                   INCLUSIVE, null, 8),
             ordering(ASC, DESC),
             1001);
    }

    @Ignore
    @Test
    public void testGENull()
    {
        test(range(INCLUSIVE, null, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, null, null,
                   INCLUSIVE, null, 8),
             ordering(ASC, ASC),
             1000, 1001);
    }

    @Ignore
    @Test
    public void testLTNull()
    {
        test(range(INCLUSIVE, 5, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED),
             ordering(ASC),
             1002, 1003);
        test(range(INCLUSIVE, null, 8,
                   EXCLUSIVE, null, null),
             ordering(ASC, ASC),
             1001);
    }

    @Ignore
    @Test
    public void testLENull()
    {
        // restrict one column, unidirectional
        try {
            test(range(INCLUSIVE, 5, UNSPECIFIED,
                       INCLUSIVE, null, UNSPECIFIED),
                 ordering(ASC));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        // restrict both columns, unidirectional
        try {
            test(range(INCLUSIVE, null, 8,
                       INCLUSIVE, null, null),
                 ordering(ASC, ASC));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        // restrict one column, unidirectional
        try {
            test(range(INCLUSIVE, 5, UNSPECIFIED,
                       INCLUSIVE, null, UNSPECIFIED),
                 ordering(DESC));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        // restrict both columns, unidirectional
        try {
            test(range(INCLUSIVE, null, 8,
                       INCLUSIVE, null, null),
                 ordering(DESC, DESC));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Ignore
    @Test
    public void testISNull()
    {
        test(range(INCLUSIVE, null, UNSPECIFIED,
                   INCLUSIVE, null, UNSPECIFIED),
             ordering(ASC),
             1000, 1001);
        test(range(INCLUSIVE, null, null,
                   INCLUSIVE, null, null),
             ordering(ASC, ASC),
             1000);
    }

    // For use by this class

    private void test(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        Row[] expected = new Row[expectedIds.length];
        for (int i = 0; i < expectedIds.length; i++) {
            int id = expectedIds[i];
            expected[i] = dbRow(id);
        }
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private void dump(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        dumpToAssertion(plan);
    }

    private IndexKeyRange range(boolean loInclusive, Integer aLo, Integer bLo,
                                boolean hiInclusive, Integer aHi, Integer bHi)
    {
        IndexBound lo;
        if (aLo == UNSPECIFIED) {
            lo = null;
            fail();
        } else if (bLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo), new SetColumnSelector(0));
        } else {
            lo = new IndexBound(row(idxRowType, aLo, bLo), new SetColumnSelector(0, 1));
        }
        IndexBound hi;
        if (aHi == UNSPECIFIED) {
            hi = null;
            fail();
        } else if (bHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi), new SetColumnSelector(0));
        } else {
            hi = new IndexBound(row(idxRowType, aHi, bHi), new SetColumnSelector(0, 1));
        }
        return IndexKeyRange.bounded(idxRowType, lo, loInclusive, hi, hiInclusive);
    }

    private API.Ordering ordering(boolean... directions)
    {
        assertTrue(directions.length >= 1 && directions.length <= 2);
        API.Ordering ordering = API.ordering();
        if (directions.length >= 1) {
            ordering.append(field(idxRowType, A), directions[0]);
        }
        if (directions.length >= 2) {
            ordering.append(field(idxRowType, B), directions[1]);
        }
        return ordering;
    }

    private Row dbRow(long id)
    {
        for (Row newRow : db) {
            if (newRow.value(0).getInt64() == id) {
                return row(idxRowType, newRow.value(1).getInt64(), newRow.value(2).getInt64(), newRow.value(0).getInt64());
            }
        }
        fail();
        return null;
    }

    // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int ID = 3;
    private static final boolean ASC = true;
    private static final boolean DESC = false;
    private static final boolean EXCLUSIVE = false;
    private static final boolean INCLUSIVE = true;
    private static final Integer UNSPECIFIED = new Integer(Integer.MIN_VALUE); // Relying on == comparisons

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
}
