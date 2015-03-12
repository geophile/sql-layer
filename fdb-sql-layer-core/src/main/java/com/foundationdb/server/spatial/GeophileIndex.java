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

import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.geophile.z.Cursor;
import com.geophile.z.DuplicateRecordException;
import com.geophile.z.Index;
import com.geophile.z.Record;

import java.io.IOException;

public class GeophileIndex extends Index<IndexRow>
{
    // Index interface

    @Override
    public void add(IndexRow record) throws IOException, InterruptedException, DuplicateRecordException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(long z, Record.Filter<IndexRow> recordFilter) throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<IndexRow> cursor() throws IOException, InterruptedException
    {
        return cursorFactory.newCursor(this);
    }

    @Override
    public IndexRow newRecord()
    {
        return adapter.takeIndexRow(indexRowType);
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

    // GeophileIndex interface

    public GeophileIndex(StoreAdapter adapter, IndexRowType indexRowType, CursorFactory cursorFactory)
    {
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        this.cursorFactory = cursorFactory;
    }

    // Object state

    private final StoreAdapter adapter;
    private final IndexRowType indexRowType;
    private final CursorFactory cursorFactory;

    public interface CursorFactory
    {
        public GeophileCursor newCursor(GeophileIndex geophileIndex);
    }
}
