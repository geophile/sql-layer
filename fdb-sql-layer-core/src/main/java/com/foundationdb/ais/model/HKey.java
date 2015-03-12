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
package com.foundationdb.ais.model;

import java.util.ArrayList;
import java.util.List;

public class HKey
{
    @Override
    public synchronized String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append("HKey(");
        boolean firstTable = true;
        for (HKeySegment segment : segments) {
            if (firstTable) {
                firstTable = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(segment.toString());
        }
        buffer.append(")");
        return buffer.toString();
    }
    
    public Table table()
    {
        return table;
    }

    public List<HKeySegment> segments()
    {
        return segments;
    }

    public int nColumns()
    {
        ensureDerived();
        return columns.length;
    }
    
    public Column column(int i)
    {
        ensureDerived();
        return columns[i];
    }

    public HKey(Table table)
    {
        this.table = table;
    }

    public synchronized HKeySegment addSegment(Table segmentTable)
    {
        assert keyDepth == null : segmentTable; // Should only be computed after HKeySegments are completely formed.
        Table lastSegmentTable = segments.isEmpty() ? null : segments.get(segments.size() - 1).table();
        assert segmentTable.getParentTable() == lastSegmentTable;
        HKeySegment segment = new HKeySegment(this, segmentTable);
        segments.add(segment);
        return segment;
    }

    public boolean containsColumn(Column column) 
    {
        ensureDerived();
        for (Column c : columns) {
            if (c == column) {
                return true;
            }
        }
        return false;
    }

    public int[] keyDepth()
    {
        ensureDerived();
        return keyDepth;
    }

    // For use by this class

    private void ensureDerived()
    {
        if (columns == null) {
            synchronized (this) {
                if (columns == null) {
                    // columns
                    List<Column> columnList = new ArrayList<>();
                    for (HKeySegment segment : segments) {
                        for (HKeyColumn hKeyColumn : segment.columns()) {
                            columnList.add(hKeyColumn.column());
                        }
                    }
                    
                    Column[] columnsTmp = new Column[columnList.size()];
                    columnsTmp = columnList.toArray(columnsTmp);
                    // keyDepth
                    int[] keyDepthTmp = new int[segments.size() + 1];
                    int hKeySegments = segments.size();
                    for (int hKeySegment = 0; hKeySegment < hKeySegments; hKeySegment++) {
                        keyDepthTmp[hKeySegment] =
                            hKeySegment == 0
                            ? 1
                            // + 1 to account for the ordinal
                            : keyDepthTmp[hKeySegment - 1] + 1 + segments.get(hKeySegment - 1).columns().size();
                    }
                    keyDepthTmp[hKeySegments] = columnsTmp.length + hKeySegments;
                    keyDepth = keyDepthTmp;
                    columns = columnsTmp;
                }
            }
        }
    }

    // State

    private final Table table;
    private final List<HKeySegment> segments = new ArrayList<>();
    // keyDepth[n] is the number of key segments (ordinals + key values) comprising an hkey of n parts.
    // E.g. keyDepth[1] for the number of segments of the root hkey.
    //      keyDepth[2] for the number of key segments of the root's child + keyDepth[1].
    private volatile int[] keyDepth;
    private volatile Column[] columns;
}
