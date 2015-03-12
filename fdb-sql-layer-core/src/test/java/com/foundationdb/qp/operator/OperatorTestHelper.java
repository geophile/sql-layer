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
package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.Strings;
import com.foundationdb.util.tap.InOutTap;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class OperatorTestHelper {

    // OperatorTestHelper interface

    public static void check(Operator plan, Collection<? extends Row> expecteds, RowCheck additionalCheck) {
        List<Row> actuals = execute(plan);
        if (expecteds.size() != actuals.size()) {
            assertEquals("output", Strings.join(expecteds), Strings.join(actuals));
            assertEquals("size (expecteds=" + expecteds+", actuals=" + actuals + ')', expecteds.size(), actuals.size());
        }
        int rowCount = 0;
        Iterator<? extends Row> expectedsIter = expecteds.iterator();
        for (Row actual : actuals) {
            Row expected = expectedsIter.next();
            int actualWidth = actual.rowType().nFields();
            assertEquals("row width", expected.rowType().nFields(), actualWidth);
            for (int i = 0; i < actualWidth; ++i) {
                checkRowInstance(expected, actual, i, rowCount, actuals, expecteds);
            }
            if (additionalCheck != null)
                additionalCheck.check(actual);
            ++rowCount;
        }
   }
    
    private static void checkRowInstance(Row expected, Row actual, int i, int rowCount, List<Row> actuals, Collection<? extends Row> expecteds) {   
        ValueSource actualSource = actual.value(i);
        ValueSource expectedSource = expected.value(i);
        TInstance actualType = actual.rowType().typeAt(i);
        TInstance expectedType = expected.rowType().typeAt(i);
        if (actualType == null || expectedType == null) {
            assert actualSource.isNull() && expectedSource.isNull();
            return;
        }
        assertTrue(expectedType + " != " + actualType, expectedType.equalsExcludingNullable(actualType));

        
        if(!TClass.areEqual(actualSource, expectedSource) &&
           !(actualSource.isNull() && expectedSource.isNull())) {
            Assert.assertEquals(
                    String.format("row[%d] field[%d]", rowCount, i),
                    str(expecteds),
                    str(actuals));
            assertEquals(String.format("row[%d] field[%d]", rowCount, i), expectedSource, actualSource);
            throw new AssertionError("should have failed by now!");
        }
    }

    public static void check(Operator plan, Collection<? extends Row> expecteds) {
        check(plan, expecteds, null);
    }

    public static Cursor open(Operator plan) {
        QueryContext queryContext = new SimpleQueryContext(ADAPTER);
        QueryBindings queryBindings = queryContext.createBindings();
        QueryBindingsCursor queryBindingsCursor = new SingletonQueryBindingsCursor(queryBindings);
        Cursor result = plan.cursor(queryContext, queryBindingsCursor);
        reopen(result);
        return result;
    }

    public static void reopen(Cursor cursor) {
        cursor.openTopLevel();
    }

    public static List<Row> execute(Operator plan) {
        List<Row> rows = new ArrayList<>();
        Cursor cursor = open(plan);
        try {
            for(Row row = cursor.next(); row != null; row = cursor.next()) {
                rows.add(row);
            }
            return rows;
        } finally {
            cursor.close();
        }
    }

    public static Schema schema() {
        return new Schema(new com.foundationdb.ais.model.AkibanInformationSchema());
    }

    // for use in this class

    private static String str(Collection<? extends Row> rows) {
        return Strings.join(rows);
    }

    private OperatorTestHelper() {}

    // "const"s

    static final TestAdapter ADAPTER = new TestAdapter();

    // nested classes

    public interface RowCheck {
        void check(Row row);
    }

    private static class TestAdapter extends StoreAdapter
    {
        @Override
        public GroupCursor newGroupCursor(Group group)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor newIndexCursor(QueryContext context,
                                     IndexRowType indexType,
                                     IndexKeyRange keyRange,
                                     API.Ordering ordering,
                                     IndexScanSelector selector,
                                     boolean openAllSubCursors)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Store getUnderlyingStore() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateRow(Row oldRow, Row newRow)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeRow(Row newRow, Collection<TableIndex> indexes, Collection<GroupIndex> groupIndexes)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRow(Row oldRow, boolean cascadeDefault)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Sorter createSorter(QueryContext context,
                                   QueryBindings bindings,
                           RowCursor input,
                           RowType rowType,
                           API.Ordering ordering,
                           API.SortOption sortOption,
                           InOutTap loadTap)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getQueryTimeoutMilli()
        {
            return -1;
        }

        @Override
        public long rowCount(Session session, RowType tableType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long sequenceNextValue(Sequence sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long sequenceCurrentValue(Sequence sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexRow takeIndexRow(IndexRowType indexRowType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void returnIndexRow(IndexRow indexRow) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IterationHelper createIterationHelper(IndexRowType indexRowType) {
            throw new UnsupportedOperationException();
        }

        public TestAdapter()
        {
            super(null, null);
        }

        @Override
        public IndexRow newIndexRow(IndexRowType indexRowType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyCreator getKeyCreator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public AkibanInformationSchema getAIS() {
            throw new UnsupportedOperationException();
        }
    }
}
