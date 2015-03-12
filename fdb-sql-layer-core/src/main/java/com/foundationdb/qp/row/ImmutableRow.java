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

import com.foundationdb.qp.rowtype.CompoundRowType;
import com.foundationdb.qp.rowtype.FlattenedRowType;
import com.foundationdb.qp.rowtype.ProductRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class ImmutableRow extends AbstractValuesHolderRow
{
    public ImmutableRow(Row row)
    {
        this(row.rowType(), getValueSources(row));
    }

    public ImmutableRow(RowType rowType, Iterator<? extends ValueSource> initialValues)
    {
        super(rowType, false, initialValues);
    }

    public static Iterator<ValueSource> getValueSources(Row row)
    {
        int size = row.rowType().nFields();
        List<ValueSource> ret = new ArrayList<>(size);
        for (int i = 0; i < size; ++i)
            ret.add(row.value(i));
        return ret.iterator();
    }

    public static Row buildImmutableRow(Row row) {
        if (!row.isBindingsSensitive()) {
            return row;
        }
        if (row instanceof FlattenedRow) {
            FlattenedRow fRow = (FlattenedRow) row;
            return new FlattenedRow((FlattenedRowType)fRow.rowType(), buildImmutableRow(fRow.getFirstRow()), buildImmutableRow(fRow.getSecondRow()), fRow.hKey());
        }
        else if (row instanceof ProductRow) {
            ProductRow pRow = (ProductRow) row;
            return new ProductRow((ProductRowType)pRow.rowType(), buildImmutableRow(pRow.getFirstRow()), buildImmutableRow(pRow.getSecondRow()));
        }
        else if (row instanceof CompoundRow) {
            CompoundRow cRow = (FlattenedRow) row;
            return new CompoundRow((CompoundRowType)cRow.rowType(), buildImmutableRow(cRow.getFirstRow()), buildImmutableRow(cRow.getSecondRow()));
        }
        else {
            return new ImmutableRow(row);
        }
    }
}
