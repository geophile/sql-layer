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

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.storeadapter.indexcursor.IndexCursor;
import com.geophile.z.Cursor;
import com.geophile.z.space.SpaceImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GeophileCursor extends Cursor<IndexRow>
{
    // Cursor interface

    @Override
    public IndexRow next() throws IOException, InterruptedException
    {
        if (currentCursor == null) {
            // A cursor should have been registered with z-value corresponding to the entire space,
            // Z_MIN = (0x0, 0).
            currentCursor = cursors.get(SpaceImpl.Z_MIN);
            assert currentCursor != null;
        }
        return currentCursor.next();
    }

    @Override
    public void goTo(IndexRow key) throws IOException, InterruptedException
    {
        long z = key.z();
        currentCursor = cursors.get(z);
        assert currentCursor != null : SpaceImpl.formatZ(z);
        if (!openEarly) {
            currentCursor.open(); // open is idempotent
        }
        currentCursor.jump(key, null);
    }

    @Override
    public boolean deleteCurrent() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    // GeophileCursor interface

    public void addCursor(long z, IndexCursor indexCursor)
    {
        if (currentCursor != null) {
            // Shouldn't add a cursor after the cursor has been opened.
            throw new IllegalArgumentException();
        }
        CachingCursor cachingCursor = new CachingCursor(z, indexCursor);
        if (openEarly) {
            cachingCursor.open();
        }
        cursors.put(z, cachingCursor);
    }

    public void rebind(QueryBindings bindings)
    {
        for (CachingCursor cursor : cursors.values()) {
            cursor.rebind(bindings);
        }
    }

    public void close()
    {
        for (CachingCursor cursor : cursors.values()) {
            cursor.close();
        }
    }

    public GeophileCursor(GeophileIndex index, boolean openEarly)
    {
        super(index);
        this.openEarly = openEarly;
    }

    // Object state

    private final boolean openEarly;
    private final Map<Long, CachingCursor> cursors = new HashMap<>(); // z -> CachingCursor
    private CachingCursor currentCursor;
}
