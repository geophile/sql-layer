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
package com.foundationdb.server.service.externaldata;

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;

import java.util.ArrayList;
import java.util.List;

public abstract class GenericRowTracker implements RowTracker {
    private final List<RowType> openTypes = new ArrayList<>(3);
    private RowType curRowType;
    private int curDepth;

    protected void setDepth(int depth) {
        curDepth = depth;
    }

    @Override
    public void reset() {
        curRowType = null;
        curDepth = 0;
        openTypes.clear();
    }

    @Override
    public int getMinDepth() {
        return 0;
    }

    @Override
    public int getMaxDepth() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void beginRow(Row row) {
        curRowType = row.rowType();
    }

    @Override
    public int getRowDepth() {
        return curDepth;
    }

    @Override
    public boolean isSameRowType() {
        return (getRowDepth() < openTypes.size()) &&
               (curRowType == openTypes.get(getRowDepth()));
    }

    @Override
    public void pushRowType() {
        openTypes.add(curRowType);
    }

    @Override
    public void popRowType() {
    }
}
