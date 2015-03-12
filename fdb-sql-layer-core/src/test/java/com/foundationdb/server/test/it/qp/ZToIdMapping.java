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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class ZToIdMapping implements Iterable<Map.Entry<Long, List<Integer>>>
{
    @Override
    public Iterator<Map.Entry<Long, List<Integer>>> iterator()
    {
        return zToId.entrySet().iterator();
    }

    public int size()
    {
        return count;
    }

    public void add(long z, int id)
    {
        List<Integer> zIds = zToId.get(z);
        if (zIds == null) {
            zIds = new ArrayList<>();
            zToId.put(z, zIds);
        }
        assert !zIds.contains(id);
        zIds.add(id);
        ids.add(id);
        count++;
    }

    public List<Integer> ids()
    {
        return ids;
    }

    public void clear()
    {
        zToId.clear();
        ids.clear();
        count = 0;
    }

    public long[][] toArray(ExpectedRowCreator rowCreator)
    {
        return toArray(count, rowCreator);
    }

    public long[][] toArray(int expectedRows, ExpectedRowCreator rowCreator)
    {
        long[][] array = new long[expectedRows][];
        int r = 0;
        for (Map.Entry<Long, List<Integer>> entry : zToId.entrySet()) {
            long z = entry.getKey();
            for (Integer id : entry.getValue()) {
                long[] fields = rowCreator.fields(z, id);
                if (fields != null) {
                    array[r++] = fields;
                }
            }
        }
        return
            r == expectedRows
            ? array
            : Arrays.copyOf(array, r);
    }

    public interface ExpectedRowCreator
    {
        long[] fields(long z, int id);
    }

    private Map<Long, List<Integer>> zToId = new TreeMap<>();
    private List<Integer> ids = new ArrayList<>();
    private int count = 0;
}
