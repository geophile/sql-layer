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

public class HKeySegment
{
    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(table().getName().getTableName());
        buffer.append(": (");
        boolean firstColumn = true;
        for (HKeyColumn hKeyColumn : columns()) {
            if (firstColumn) {
                firstColumn = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(hKeyColumn.toString());
        }
        buffer.append(")");
        return buffer.toString();
    }

    public HKeySegment(HKey hKey, Table table)
    {
        this.hKey = hKey;
        this.table = table;
        if (hKey.segments().isEmpty()) {
            this.positionInHKey = 0;
        } else {
            HKeySegment lastSegment = hKey.segments().get(hKey.segments().size() - 1);
            this.positionInHKey =
                lastSegment.columns().isEmpty()
                ? lastSegment.positionInHKey() + 1
                : lastSegment.columns().get(lastSegment.columns().size() - 1).positionInHKey() + 1;
        }
    }
    
    public HKey hKey()
    {
        return hKey;
    }

    public Table table()
    {
        return table;
    }

    public int positionInHKey()
    {
        return positionInHKey;
    }

    public List<HKeyColumn> columns()
    {
        return columns;
    }

    public HKeyColumn addColumn(Column column)
    {
        assert column != null;
        HKeyColumn hKeyColumn = new HKeyColumn(this, column);
        columns.add(hKeyColumn);
        return hKeyColumn;
    }

    private final HKey hKey;
    private final Table table;
    private final List<HKeyColumn> columns = new ArrayList<>();
    private final int positionInHKey;
}
