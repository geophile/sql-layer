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

import com.geophile.z.Record;
import com.geophile.z.SpatialObject;
import com.geophile.z.index.RecordWithSpatialObject;

import java.util.Comparator;

public class TestRecord extends RecordWithSpatialObject
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("(%s: %s)", soid, super.toString());
    }

    @Override
    public int hashCode()
    {
        return super.hashCode() ^ soid;
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean eq = false;
        if (super.equals(obj) && obj instanceof TestRecord) {
            TestRecord that = (TestRecord) obj;
            eq = this.soid == that.soid;
        }
        return eq;
    }

    // Record interface

    @Override
    public void copyTo(Record record)
    {
        super.copyTo(record);
        ((TestRecord)record).soid = this.soid;
    }

    // TestRecord interface

    public int soid()
    {
        return soid;
    }

    public void soid(int newId)
    {
        soid = newId;
    }

    public TestRecord(SpatialObject spatialObject)
    {
        spatialObject(spatialObject);
        soid = 0;
    }

    public TestRecord(SpatialObject spatialObject, int soid)
    {
        spatialObject(spatialObject);
        this.soid = soid;
    }

    public TestRecord()
    {}

    // Class state

    public static final Comparator<TestRecord> COMPARATOR =
        new Comparator<TestRecord>()
        {
            @Override
            public int compare(TestRecord r, TestRecord s)
            {
                return
                    r.z() < s.z()
                    ? -1
                    : r.z() > s.z()
                      ? 1
                      : r.soid < s.soid
                        ? -1
                        : r.soid > s.soid
                          ? 1
                          : 0;
            }
        };

    private static final int UNDEFINED_SOID = -1;

    // Object state

    private int soid = UNDEFINED_SOID;

    // Inner classes

    public static class Factory implements Record.Factory<TestRecord>
    {
        @Override
        public TestRecord newRecord()
        {
            return new TestRecord(spatialObject, id);
        }

        public Factory setup(SpatialObject spatialObject, int id)
        {
            this.spatialObject = spatialObject;
            this.id = id;
            return this;
        }

        private SpatialObject spatialObject;
        private int id;
    }
}
