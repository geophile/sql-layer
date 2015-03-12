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
package com.foundationdb.qp.row;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.server.types.TInstance;
import com.geophile.z.Record;
import com.geophile.z.Space;
import com.persistit.Key;
import com.persistit.Value;

public abstract class IndexRow extends AbstractRow implements com.geophile.z.Record
{
    // Row interface

    @Override
    public IndexRowType rowType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // IndexRow interface

    public abstract <S> void append(S source, TInstance type);
    public abstract void append (EdgeValue value);

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }

    public abstract void resetForRead(Index index, Key key, Value value); 
    public abstract void resetForWrite(Index index, Key key);
    
    public abstract int compareTo(IndexRow startKey, int startBoundColumns,
            boolean[] ascending); 
    
    public abstract int compareTo(IndexRow thatKey, int startBoundColumns);

    public abstract void reset();
    
    public abstract boolean keyEmpty();
    
    // Group Index Row only - table bitmap stored in index value
    public abstract void tableBitmap(long bitmap);
    public abstract long tableBitmap(); 

    // com.geophile.z.Record interface

    @Override
    public final long z()
    {
        // z is set only for a query object. If it hasn't been set, then we have an index row
        // with a field containing a z-value.
        if (z == Space.Z_NULL) {
            z = this.uncheckedValue(zPosition()).getInt64();
        }
        return z;
    }

    @Override
    public final void z(long z)
    {
        this.z = z;
    }

    @Override
    public final void copyTo(Record record)
    {
        throw new UnsupportedOperationException();
    }

    // TODO: Remove these as we get rid of the Key use in the upper layers
    
    public abstract void copyPersistitKeyTo(Key key);
    public abstract void appendFieldTo(int position, Key target);
    public abstract void copyFrom(Key key, Value value);

    protected abstract int zPosition();

    private long z = Space.Z_NULL;

    public static enum EdgeValue {
        BEFORE,
        AFTER;
    }
}
