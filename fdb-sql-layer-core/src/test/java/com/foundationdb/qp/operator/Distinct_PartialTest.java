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
package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.TPreparedField;
import static com.foundationdb.qp.operator.API.*;

import org.junit.Test;

import java.util.Deque;

public class Distinct_PartialTest {

    @Test
    public void testDistinct() {
        Operator input = new TestOperator(new RowsBuilder(MNumeric.INT.instance(true), MString.varchar(), MNumeric.INT.instance(true))
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(2L,"xyz",0L)
            .row(2L,"xyz",0L)
            .row(2L,"xyz",1L)
            .row(3L,"def",0L)
            .row(3L,"def",null)
        );
        Operator plan = distinct_Partial(input, input.rowType());
        Deque<Row> expected = new RowsBuilder(MNumeric.INT.instance(true), MString.varchar(), MNumeric.INT.instance(true))
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(2L,"xyz",0L)
            .row(2L,"xyz",1L)
            .row(3L,"def",0L)
            .row(3L,"def",null)
            .rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void testPartial() {
        Operator input = new TestOperator(new RowsBuilder(MNumeric.INT.instance(true), MString.varchar(), MNumeric.INT.instance(true))
            .row(1L,"abc",0L)
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(1L,"abc",0L)
        );
        Operator plan = distinct_Partial(input, input.rowType());
        Deque<Row> expected = new RowsBuilder(MNumeric.INT.instance(true), MString.varchar(), MNumeric.INT.instance(true))
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(1L,"abc",0L)
            .rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void testWithSort() {
        Operator input = new TestOperator(new RowsBuilder(MNumeric.INT.instance(true), MString.varchar())
            .row(7L,"def")
            .row(3L,"def")
            .row(0L,"abc")
            .row(9L,"xyz")
            .row(3L,"xyz")
            .row(2L,"xyz")
            .row(9L,"xyz")
            .row(0L,"abc")
            .row(2L,"xyz")
            .row(6L,"def")
            .row(7L,"def")
            .row(6L,"ghi")
            .row(2L,"abc")
            .row(6L,"def")
            .row(2L,"def")
            .row(7L,"def")
            .row(8L,"ghi")
            .row(2L,"abc")
            .row(5L,"abc")
            .row(9L,"ghi")
            .row(6L,"ghi")
            .row(5L,"xyz")
            .row(5L,"ghi")
            .row(3L,"def")
            .row(7L,"abc")
            .row(7L,"abc")
            .row(4L,"abc")
            .row(0L,"ghi")
            .row(9L,"def")
            .row(3L,"def")
            .row(7L,"xyz")
            .row(4L,"def")
            .row(4L,"ghi")
            .row(4L,"abc")
            .row(8L,"abc")
            .row(6L,"def")
            .row(3L,"def")
            .row(1L,"ghi")
            .row(4L,"abc")
            .row(8L,"abc")
            .row(3L,"ghi")
            .row(8L,"abc")
            .row(8L,"abc")
            .row(8L,"abc")
            .row(8L,"abc")
            .row(9L,"ghi")
            .row(9L,"xyz")
            .row(6L,"def")
            .row(3L,"def")
            .row(2L,"ghi")
            .row(2L,"abc")
            .row(8L,"abc")
            .row(5L,"ghi")
            .row(6L,"xyz")
            .row(7L,"xyz")
            .row(0L,"def")
            .row(6L,"def")
            .row(3L,"abc")
            .row(2L,"abc")
            .row(1L,"xyz")
            .row(9L,"ghi")
            .row(1L,"def")
            .row(4L,"ghi")
            .row(5L,"def")
            .row(0L,"def")
            .row(0L,"def")
            .row(1L,"ghi")
            .row(8L,"def")
            .row(4L,"abc")
            .row(6L,"xyz")
            .row(6L,"def")
            .row(0L,"xyz")
            .row(1L,"abc")
            .row(7L,"abc")
            .row(6L,"xyz")
            .row(8L,"ghi")
            .row(7L,"xyz")
            .row(6L,"xyz")
            .row(5L,"xyz")
            .row(5L,"ghi")
            .row(5L,"xyz")
            .row(4L,"def")
            .row(4L,"xyz")
            .row(8L,"def")
            .row(2L,"def")
            .row(3L,"abc")
            .row(3L,"def")
            .row(7L,"xyz")
            .row(2L,"xyz")
            .row(2L,"def")
            .row(5L,"ghi")
            .row(8L,"ghi")
            .row(0L,"def")
            .row(2L,"ghi")
            .row(1L,"xyz")
            .row(7L,"xyz")
            .row(1L,"abc")
            .row(4L,"abc")
            .row(9L,"def")
            .row(7L,"abc")
        );
        Ordering ordering = ordering();
        ordering.append(new TPreparedField(input.rowType().typeAt(0), 0), true);
        ordering.append(new TPreparedField(input.rowType().typeAt(1), 1), true);
        Operator sort = sort_InsertionLimited(input, input.rowType(),
                                              ordering, SortOption.PRESERVE_DUPLICATES, 200);
        Operator plan = distinct_Partial(sort, sort.rowType());
        Deque<Row> expected = new RowsBuilder(MNumeric.INT.instance(true), MString.varchar())
            .row(0L,"abc")
            .row(0L,"def")
            .row(0L,"ghi")
            .row(0L,"xyz")
            .row(1L,"abc")
            .row(1L,"def")
            .row(1L,"ghi")
            .row(1L,"xyz")
            .row(2L,"abc")
            .row(2L,"def")
            .row(2L,"ghi")
            .row(2L,"xyz")
            .row(3L,"abc")
            .row(3L,"def")
            .row(3L,"ghi")
            .row(3L,"xyz")
            .row(4L,"abc")
            .row(4L,"def")
            .row(4L,"ghi")
            .row(4L,"xyz")
            .row(5L,"abc")
            .row(5L,"def")
            .row(5L,"ghi")
            .row(5L,"xyz")
            .row(6L,"def")
            .row(6L,"ghi")
            .row(6L,"xyz")
            .row(7L,"abc")
            .row(7L,"def")
            .row(7L,"xyz")
            .row(8L,"abc")
            .row(8L,"def")
            .row(8L,"ghi")
            .row(9L,"def")
            .row(9L,"ghi")
            .row(9L,"xyz")
            .rows();
        OperatorTestHelper.check(plan, expected);
    }
}
