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

import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.MultiCursor;
import com.foundationdb.server.api.dml.ColumnSelector;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MultiCursorIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null primary key");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    @Test(expected=IllegalStateException.class)
    public void testSealed()
    {
        MultiCursor multiCursor = multiCursor();
        multiCursor.open();
        multiCursor.addCursor(new TestCursor(new int[]{}));
    }

    @Test
    public void testNoCursors()
    {
        RowCursor multiCursor = multiCursor();
        multiCursor.open();
        assertTrue(multiCursor.isIdle());
        assertNull(multiCursor.next());
    }

    @Test
    public void testOneCursor()
    {
        for (int n = 0; n < 10; n++) {
            int[] a = new int[n];
            for (int i = 0; i < n; i++) {
                a[i] = i;
            }
            RowCursor multiCursor = multiCursor(new TestCursor(a));
            multiCursor.open();
            assertTrue(multiCursor.isActive());
            Row row;
            long expected = 0;
            while ((row = multiCursor.next()) != null) {
                assertEquals(expected, unwrap(row));
                expected++;
            }
            assertEquals(n, expected);
            assertTrue(multiCursor.isIdle());
        }
    }

    @Test
    public void testMultipleCursors()
    {
        testMultipleCursors(false);
    }

    @Test
    public void testOpenAll()
    {
        testMultipleCursors(true);
    }

    private void testMultipleCursors(boolean openAll) {
        RowCursor multiCursor = multiCursor(openAll,
                                            new TestCursor(new int[]{}),
                                            new TestCursor(new int[]{0, 1, 2}),
                                            new TestCursor(new int[]{}),
                                            new TestCursor(new int[]{}),
                                            new TestCursor(new int[]{3}),
                                            new TestCursor(new int[]{}),
                                            new TestCursor(new int[]{}));
        multiCursor.open();
        Row row;
        long expected = 0;
        while ((row = multiCursor.next()) != null) {
            assertEquals(expected, unwrap(row));
            expected++;
        }
        assertEquals(4, expected);
        assertTrue(multiCursor.isIdle());
    }

    private MultiCursor multiCursor(TestCursor ... cursors)
    {
        return multiCursor(false, cursors);
    }

    private MultiCursor multiCursor(boolean openAll, TestCursor ... cursors)
    {
        MultiCursor multiCursor = new MultiCursor(openAll);
        for (TestCursor cursor : cursors) {
            multiCursor.addCursor(cursor);
        }
        return multiCursor;
    }

    private int unwrap(Row row)
    {
        return getLong(row, 0).intValue();
    }

    private int t;
    private TableRowType tRowType;

    private class TestCursor implements RowCursor
    {
        @Override
        public void open()
        {
            position = 0;
        }

        @Override
        public Row next()
        {
            Row row = null;
            if (position < items.length) {
                row = row();
                position++;
            }
            return row;
        }

        @Override
        public void jump(Row row, ColumnSelector columnSelector)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            position = items == null ? 0 : items.length;
        }

        @Override
        public boolean isIdle()
        {
            return position == items.length;
        }

        @Override
        public boolean isActive()
        {
            return position < items.length;
        }

        // TestCursor interface

        public TestCursor(int[] items)
        {
            this.items = items;
        }

        // For use by this class

        public Row row()
        {
            return new ValuesHolderRow(tRowType, items[position]);
        }

        // Object state

        private int[] items;
        private int position;
        @Override
        public boolean isClosed() {
            return items == null || position == items.length;
        }

        @Override
        public void setIdle() {
            
        }
    }
}
