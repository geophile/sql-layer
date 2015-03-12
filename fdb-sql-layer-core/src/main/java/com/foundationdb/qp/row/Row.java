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

import com.foundationdb.ais.model.Table;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.util.Debug;

public interface Row extends ValueRecord
{
    /**
     * Setting this to true causes every value type to be checked against the rowtype,
     * to make sure they're the same. This can be rather expensive, and happens for every
     * value examined. Generally speaking the rest of the code does a good enough job that
     * this is rare at best, but it can act as a canary for bigger problems.
     */
    final boolean DEBUG_ROWTYPE = Debug.isOn("row_type");

    RowType rowType();
    HKey hKey();
    HKey ancestorHKey(Table table);
    boolean ancestorOf(Row that);
    boolean containsRealRowOf(Table table);
    Row subRow(RowType subRowType);
    boolean isBindingsSensitive();

    /**
     * Compares two rows and indicates if and where they differ.
     * @param row The row to be compared to this row.
     * @param leftStartIndex First field to compare in this row.
     * @param rightStartIndex First field to compare in the other row.
     * @param fieldCount Number of fields to compare.
     * @return 0 if all fields are equal. A negative value indicates that this row had the first field
     * that was not equal to the corresponding field in the other row. A positive value indicates that the
     * other row had the first field that was not equal to the corresponding field in this row. In both non-zero
     * cases, the absolute value of the return value is the position of the field that differed, starting the numbering
     * at 1. E.g. a return value of -2 means that the first fields of the rows match, and that in the second field,
     * this row had the smaller value.
     */
    int compareTo(Row row, int leftStartIndex, int rightStartIndex, int fieldCount);
}
