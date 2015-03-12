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

import java.util.Collections;
import java.util.List;

public class HKeyColumn
{
    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(column().getTable().getName().getTableName());
        buffer.append('.');
        buffer.append(column().getName());
        return buffer.toString();
    }

    public HKeySegment segment()
    {
        return segment;
    }

    public Column column()
    {
        return column;
    }

    public List<Column> equivalentColumns()
    {
        return equivalentColumns;
    }

    public int positionInHKey()
    {
        return positionInHKey;
    }

    public HKeyColumn(HKeySegment segment, Column column)
    {
        this.segment = segment;
        this.column = column;
        this.positionInHKey = segment.positionInHKey() + segment.columns().size() + 1;
        this.equivalentColumns = Collections.unmodifiableList(column.getTable().matchingColumns(column));
    }

    // For use by this class
    
    private void findDependentTables(Column column, Table table, List<Table> dependentTables)
    {
        boolean dependent = false;
        for (HKeySegment segment : table.hKey().segments()) {
            for (HKeyColumn hKeyColumn : segment.columns()) {
                dependent = dependent || hKeyColumn.column() == column;
            }
        }
        if (dependent) {
            dependentTables.add(table);
        }
        for (Join join : table.getChildJoins()) {
            findDependentTables(column, join.getChild(), dependentTables);
        }
    }

    // State

    private final HKeySegment segment;
    private final Column column;
    private final int positionInHKey;
    // If column is a group table column, then we need to know all columns in the group table that are constrained
    // to have matching values, e.g. customer$cid and order$cid. For a user table, equivalentColumns contains just
    // column.
    private final List<Column> equivalentColumns;
}
