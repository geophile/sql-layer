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

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

public class TreeIndexCursor extends Cursor<TestRecord>
{
    // Cursor interface

    @Override
    public TestRecord next() throws IOException, InterruptedException
    {
        return neighbor();
    }

    @Override
    public void goTo(TestRecord key)
    {
        this.startAt = key;
        state(State.NEVER_USED);
    }

    @Override
    public boolean deleteCurrent() throws IOException, InterruptedException
    {
        boolean removed = false;
        if (state() == State.IN_USE) {
            treeIterator.remove();
            removed = true;
        }
        return removed;
    }

    // TreeIndexCursor interface

    public TreeIndexCursor(TreeIndex treeIndex)
    {
        super(treeIndex);
        this.tree = treeIndex.tree();
    }

    // For use by this class

    private TestRecord neighbor() throws IOException, InterruptedException
    {
        switch (state()) {
            case NEVER_USED:
                startIteration(true);
                break;
            case IN_USE:
                break;
            case DONE:
                assert current() == null;
                return null;
        }
        if (treeIterator.hasNext()) {
            TestRecord neighbor = treeIterator.next();
            current(neighbor);
            startAt = neighbor;
            state(State.IN_USE);
        } else {
            close();
        }
        return current();
    }

    private void startIteration(boolean includeStartKey)
    {
        treeIterator = tree.tailSet(startAt, includeStartKey).iterator();
    }

    // Object state

    private final TreeSet<TestRecord> tree;
    private TestRecord startAt;
    private Iterator<TestRecord> treeIterator;
}
