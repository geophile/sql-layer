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

import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.value.ValueSources;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertEquals;

public class UniqueIndexScanJumpUnboundedIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null primary key",
            "a int",
            "b int",
            "c int");
        createUniqueIndex("schema", "t", "idx", "a", "b", "c", "id");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new Row[] {
            row(t, 1000L, null, null, null),
            row(t, 1001L, null, null, 5L),
            row(t, 1002L, null, 4L, null),
            row(t, 1003L, null, 4L, 5L),
            row(t, 1010L, 1L, 11L, 111L),
            row(t, 1011L, 1L, 11L, 112L),
            row(t, 1012L, 1L, 12L, 121L),
            row(t, 1013L, 1L, 12L, 122L),
            row(t, 1020L, 2L, 21L, 211L),
            row(t, 1021L, 2L, 21L, 212L),
            row(t, 1022L, 2L, 22L, 221L),
            row(t, 1023L, 2L, 22L, 222L),
            row(t, 1030L, 3L, null, null),
            row(t, 1031L, 3L, null, 5L),
            row(t, 1032L, 3L, 4L, null),
            row(t, 1033L, 3L, 4L, 5L),
            // Duplicates of rows with nulls, which must be preserved in unique indexes
            row(t, 2000L, null, null, null),
            row(t, 2001L, null, null, 5L),
            row(t, 2002L, null, 4L, null),
            row(t, 2003L, null, 4L, 5L),
            row(t, 2030L, 3L, null, null),
            row(t, 2031L, 3L, null, 5L),
            row(t, 2032L, 3L, 4L, null),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
        for (Row row : db) {
            indexRowMap.put(ValueSources.getLong(row.value(0)),
                            new TestRow(tRowType,
                                        ValueSources.toObject(row.value(1)),    // a
                                        ValueSources.toObject(row.value(2)),    // b
                                        ValueSources.toObject(row.value(3)),    // c
                                        ValueSources.toObject(row.value(0))));  // id
        }
    }

    @Test
    public void testAAAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, ASC, ID, ASC));
        long[] idOrdering = longs(1000, 2000, 1001, 2001, 1002, 2002, 1003, 2003,
                                  1010, 1011, 1012, 1013,
                                  1020, 1021, 1022, 1023,
                                  1030, 2030, 1031, 2031, 1032, 2032, 1033);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, -1);
        cursor.closeTopLevel();
    }

    @Test
    public void testDDDD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, DESC, ID, DESC));
        long[] idOrdering = longs(1033, 2032, 1032, 2031, 1031, 2030, 1030,
                                  1023, 1022, 1021, 1020,
                                  1013, 1012, 1011, 1010,
                                  2003, 1003, 2002, 1002, 2001, 1001, 2000, 1000);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        testJump(cursor, idOrdering, 0);
        testJump(cursor, idOrdering, 1);
        cursor.closeTopLevel();
    }

    private void testJump(Cursor cursor, long[] idOrdering, int nudge)
    {
        for (int start = 0; start < idOrdering.length; start++) {
            TestRow target = indexRow(idOrdering[start]);
            // Add nudge to last field
            OverlayingRow nudgedTarget = new OverlayingRow(target);
            nudgedTarget.overlay(3, (long)(target.value(3).getInt32() + nudge));
            cursor.jump(nudgedTarget, INDEX_ROW_SELECTOR);
            Row row;
            List<Long> actualIds = new ArrayList<>();
            while ((row = cursor.next()) != null) {
                actualIds.add(getLong(row, 3));
            }
            List<Long> expectedIds = new ArrayList<>();
            for (int i = start; i < idOrdering.length; i++) {
                expectedIds.add(idOrdering[i]);
            }
            assertEquals(expectedIds, actualIds);
        }
    }

    private TestRow indexRow(long id)
    {
        return indexRowMap.get(id);
    }

    private long[] longs(long... longs)
    {
        return longs;
    }

    private IndexKeyRange unbounded()
    {
        return IndexKeyRange.unbounded(idxRowType);
    }

    private API.Ordering ordering(Object... ord) // alternating column positions and asc/desc
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

    // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int ID = 3;
    private static final boolean ASC = true;
    private static final boolean DESC = false;
    private static final SetColumnSelector INDEX_ROW_SELECTOR = new SetColumnSelector(0, 1, 2, 3);

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private Map<Long, TestRow> indexRowMap = new HashMap<>();
}
