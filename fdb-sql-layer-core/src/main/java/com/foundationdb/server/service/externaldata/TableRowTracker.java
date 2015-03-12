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

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;

public class TableRowTracker implements RowTracker {
    private final int minDepth;
    private final int maxDepth;
    // This is not sufficient if orphans are possible (when
    // ancestor keys are repeated in descendants). In that case, we
    // have to save rows and check that they are ancestors of new
    // rows, discarding any that are not.
    private final RowType[] openTypes;

    private RowType curRowType;
    private Table curTable;

    public TableRowTracker(Table table, int addlDepth) {
        minDepth = table.getDepth();
        final int max[] = { minDepth };
        if (addlDepth < 0) {
            table.visit(new AbstractVisitor() {
                @Override
                public void visit(Table table) {
                    max[0] = Math.max(max[0], table.getDepth());
                }
            });
        }
        else {
            max[0] += addlDepth;
        }
        maxDepth = max[0];
        openTypes = new RowType[maxDepth+1];
    }

    @Override
    public void reset() {
        curRowType = null;
        curTable = null;
    }

    @Override
    public int getMinDepth() {
        return minDepth;
    }

    @Override
    public int getMaxDepth() {
        return maxDepth;
    }

    @Override
    public void beginRow(Row row) {
        assert row.rowType().hasTable() : "Invalid row type for TableRowTracker";
        curRowType = row.rowType();
        curTable = curRowType.table();
    }

    @Override
    public int getRowDepth() {
        return curTable.getDepth();
    }

    @Override
    public String getRowName() {
        return curTable.getNameForOutput();
    }

    @Override
    public boolean isSameRowType() {
        return curRowType == openTypes[getRowDepth()];
    }

    @Override
    public void pushRowType() {
        openTypes[getRowDepth()] = curRowType;
    }

    @Override
    public void popRowType() {
    }
}
