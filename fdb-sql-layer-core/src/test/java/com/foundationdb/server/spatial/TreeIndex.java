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

import com.geophile.z.Cursor;
import com.geophile.z.DuplicateRecordException;
import com.geophile.z.Index;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TreeIndex implements the {@link com.geophile.z.Index} interface in terms of a {@link java.util.TreeMap}.
 * A TreeIndex is not safe for use for simultaneous use by multiple threads.
 */

public class TreeIndex extends Index<TestRecord>
{
    // Object interface

    @Override
    public String toString()
    {
        return name;
    }

    // Index interface

    @Override
    public void add(TestRecord TestRecord)
    {
        TestRecord copy = newRecord();
        TestRecord.copyTo(copy);
        boolean added = tree.add(copy);
        if (!added) {
            throw new DuplicateRecordException(copy);
        }
    }

    @Override
    public boolean remove(long z, TestRecord.Filter<TestRecord> filter)
    {
        boolean foundTestRecord = false;
        boolean zMatch = true;
        Iterator<TestRecord> iterator = tree.tailSet(key(z)).iterator();
        while (zMatch && iterator.hasNext() && !foundTestRecord) {
            TestRecord TestRecord = iterator.next();
            if (TestRecord.z() == z) {
                foundTestRecord = filter.select(TestRecord);
            } else {
                zMatch = false;
            }
        }
        if (foundTestRecord) {
            iterator.remove();
        }
        return foundTestRecord;
    }

    @Override
    public Cursor<TestRecord> cursor()
    {
        return new TreeIndexCursor(this);
    }

    @Override
    public TestRecord newRecord()
    {
        return new TestRecord();
    }

    @Override
    public boolean blindUpdates()
    {
        return false;
    }

    @Override
    public boolean stableRecords()
    {
        return false;
    }

    // TreeIndex

    public TreeIndex()
    {
        this.tree = new TreeSet<>(TestRecord.COMPARATOR);
    }

    // For use by this package

    TreeSet<TestRecord> tree()
    {
        return tree;
    }

    // For use by this class

    private TestRecord key(long z)
    {
        TestRecord keyTestRecord = newRecord();
        keyTestRecord.z(z);
        return keyTestRecord;
    }

    // Class state

    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    // Object state

    private final String name = String.format("TreeIndex(%s)", idGenerator.getAndIncrement());
    private final TreeSet<TestRecord> tree;
}
