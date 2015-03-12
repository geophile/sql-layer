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
package com.foundationdb.server.spatial;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.CursorBase;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.geophile.z.Cursor;
import com.geophile.z.DuplicateRecordException;
import com.geophile.z.Index;
import com.geophile.z.Record;
import com.persistit.Key;
import com.persistit.Value;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class CachingCursorTest
{
    @Test
    public void test0() throws IOException, InterruptedException
    {
        CachingCursor cursor = cachingCursor(0);
        assertNull(cursor.next());
        cursor.jump(START, null);
        assertNull(cursor.next());
    }

    @Test
    public void test1() throws IOException, InterruptedException
    {
        CachingCursor cursor = cachingCursor(1);
        TestRecord record;
        // Get first record
        record = (TestRecord) cursor.next();
        assertNotNull(record);
        assertEquals(0, record.id);
        // Reset and get it again
        cursor.jump(START, null);
        record = (TestRecord) cursor.next();
        assertNotNull(record);
        assertEquals(0, record.id);
        // There should be nothing else
        assertNull(cursor.next());
        // Reset should fail
        try {
            cursor.jump(START, null);
            fail();
        } catch (CachingCursor.NotResettableException e) {
            // Expected
        }
    }

    @Test
    public void testMoreThanOne() throws IOException, InterruptedException
    {
        for (int n = 2; n < 10; n++) {
            CachingCursor cursor = cachingCursor(n);
            TestRecord record;
            // Get first record
            record = (TestRecord) cursor.next();
            assertNotNull(record);
            assertEquals(0, record.id);
            // Reset and get it again
            cursor.jump(START, null);
            record = (TestRecord) cursor.next();
            assertNotNull(record);
            assertEquals(0, record.id);
            // Get the next record
            record = (TestRecord) cursor.next();
            assertNotNull(record);
            assertEquals(1, record.id);
            // Reset should not work
            try {
                cursor.jump(START, null);
                fail();
            } catch (CachingCursor.NotResettableException e) {
                // Expected
            }
        }
    }

    @Test
    public void testBadReset() throws IOException, InterruptedException
    {
        CachingCursor cursor = cachingCursor(1);
        TestRecord record;
        // Get first record
        record = (TestRecord) cursor.next();
        assertNotNull(record);
        assertEquals(0, record.id);
        // Reset and get it again
        TestRecord notStart = new TestRecord(0);
        notStart.z(Z + 1);
        try {
            cursor.jump(notStart, null);
            fail();
        } catch (CachingCursor.NotResettableException e) {
            // Expected
        }
    }

    private CachingCursor cachingCursor(int n) throws IOException, InterruptedException
    {
        TestIndex index = new TestIndex();
        for (int id = 0; id < n; id++) {
            index.add(new TestRecord(id));
        }
        TestInputCursor inputCursor = new TestInputCursor(index.records);
        return new CachingCursor(Z, inputCursor);
    }

    private static final long Z = 0;
    private static final TestRecord START = new TestRecord(0);
    static
    {
        START.z(0);
    }

    private static class TestRecord extends IndexRow
    {
        @Override
        public IndexRowType rowType()
        {
            fail();
            return null;
        }

        @Override
        public HKey hKey()
        {
            fail();
            return null;
        }

        @Override
        protected ValueSource uncheckedValue(int i)
        {
            return null;
        }

        @Override
        public <S> void append(S source, TInstance type)
        {
            fail();
        }

        @Override
        public void append(EdgeValue value)
        {
            fail();
        }

        @Override
        public HKey ancestorHKey(Table table)
        {
            fail();
            return null;
        }

        @Override
        public boolean containsRealRowOf(Table table)
        {
            fail();
            return false;
        }

        @Override
        public Row subRow(RowType subRowType)
        {
            fail();
            return null;
        }

        @Override
        public boolean isBindingsSensitive()
        {
            fail();
            return false;
        }

        @Override
        public void resetForRead(com.foundationdb.ais.model.Index index, Key key, Value value)
        {
            fail();
        }

        @Override
        public void resetForWrite(com.foundationdb.ais.model.Index index, Key key)
        {
            fail();
        }

        @Override
        public int compareTo(IndexRow startKey, int startBoundColumns, boolean[] ascending)
        {
            fail();
            return 0;
        }

        @Override
        public int compareTo(IndexRow thatKey, int startBoundColumns)
        {
            fail();
            return 0;
        }

        @Override
        public void reset()
        {
            fail();
        }

        @Override
        public boolean keyEmpty()
        {
            fail();
            return false;
        }

        @Override
        public void tableBitmap(long bitmap)
        {
            fail();
        }

        @Override
        public long tableBitmap()
        {
            fail();
            return 0;
        }

        @Override
        public void copyPersistitKeyTo(Key key)
        {
            fail();
        }

        @Override
        public void appendFieldTo(int position, Key target)
        {
            fail();
        }

        @Override
        public void copyFrom(Key key, Value value)
        {
            fail();
        }

        @Override
        protected int zPosition()
        {
            fail();
            return -1;
        }

        @Override
        public int compareTo(Row row, int leftStartIndex, int rightStartIndex, int fieldCount)
        {
            fail();
            return 0;
        }

        public TestRecord(int id)
        {
            this.id = id;
        }

        private final int id;
        private long z;
    }

    private static class TestIndex extends Index<TestRecord>
    {
        @Override
        public void add(TestRecord record)
            throws IOException, InterruptedException, DuplicateRecordException
        {
            records.add(record);
        }

        @Override
        public boolean remove(long z, Record.Filter<TestRecord> recordFilter)
            throws IOException, InterruptedException
        {
            fail();
            return false;
        }

        @Override
        public Cursor<TestRecord> cursor() throws IOException, InterruptedException
        {
            fail();
            return null;
        }

        @Override
        public TestRecord newRecord()
        {
            return null;
        }

        @Override
        public boolean blindUpdates()
        {
            return false;
        }

        @Override
        public boolean stableRecords()
        {
            return true;
        }

        List<TestRecord> records = new ArrayList<>();
    }

    private static class TestInputCursor implements CursorBase<TestRecord>
    {
        @Override
        public void open()
        {
            fail();
        }

        @Override
        public TestRecord next()
        {
            return
                iterator.hasNext()
                ? iterator.next()
                : null;
        }

        @Override
        public void close()
        {
            fail();
        }

        @Override
        public boolean isIdle()
        {
            fail();
            return false;
        }

        @Override
        public boolean isActive()
        {
            fail();
            return false;
        }

        @Override
        public boolean isClosed()
        {
            fail();
            return false;
        }

        @Override
        public void setIdle()
        {
            fail();
        }

        public TestInputCursor(List<TestRecord> records)
        {
            iterator = records.iterator();
        }

        final Iterator<TestRecord> iterator;
    }
}
