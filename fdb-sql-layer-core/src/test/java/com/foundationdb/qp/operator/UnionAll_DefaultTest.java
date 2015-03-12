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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.SetWrongTypeColumns;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class UnionAll_DefaultTest {
    
    protected boolean openBoth() {
        return false;
    }

    private Schema newSchema() {
        return new Schema(new AkibanInformationSchema());
    }

    @Test
    public void unionTwoNormal() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar(), MString.varchar())
                .row(1L, "one", null)
                .row(2L, "two", null)
                .row(1L, "one", null);
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar(), MString.varchar())
                .row(3L, "three", null)
                .row(1L, "one", null)
                .row(2L, "deux", null);
        RowsBuilder expected = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar(), MString.varchar())
                .row(1L, "one", null)
                .row(2L, "two", null)
                .row(1L, "one", null)
                .row(3L, "three", null)
                .row(1L, "one", null)
                .row(2L, "deux", null);
        check(first, second, expected);
    }

    @Test
    public void firstInputEmpty() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one");
        RowsBuilder expected = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one");
        check(first, second, expected);
    }

    @Test
    public void secondInputEmpty() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one");
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder expected = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one");
        check(first, second, expected);
    }

    @Test
    public void nullPromotedInSecondRowType() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(1, "one");
        RowsBuilder second = new RowsBuilder(schema,MNumeric.INT.instance(false), MString.varchar())
                .row(2, null);
        RowsBuilder expected = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(1, "one")
                .row(2, null);
        check(first, second, expected);
    }

    @Test
    public void nullPromotedInFirstRowType() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(1, null);
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(2, "two");
        RowsBuilder expected = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(1, null)
                .row(2, "two");
        check(first, second, expected);
    }

    @Test
    public void twoOpens() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false))
                .row(1L);
        RowsBuilder second = new RowsBuilder(schema,MNumeric.INT.instance(false))
                .row(2L);
        Operator union = union(first, second);
        Cursor cursor = OperatorTestHelper.open(union);
        int count = 0;
        while(cursor.next() != null) {
            ++count;
        }
        assertEquals("count", 2, count);
        cursor.close();
        count = 0;
        OperatorTestHelper.reopen(cursor);
        while(cursor.next() != null) {
            ++count;
        }
        assertEquals("count", 2, count);
    }

    @Test
    public void bothInputsEmpty() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder expected = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        check(first, second, expected);
    }

    @Test
    public void bothInputsSameRowType() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(1L, "one");
        RowsBuilder second = new RowsBuilder(first.rowType())
                .row(2L, "two");

        RowsBuilder expected = new RowsBuilder(first.rowType())
                .row(1L, "one")
                .row(2L, "two");
        Operator union = union(first, second);
        assertSame("rowType", first.rowType(), union.rowType());
        check(first, second, expected);
    }

    @Test(expected = SetWrongTypeColumns.class)
    public void inputsNotOfRightShape() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.TEXT.instance(false));
        union(first, second);
    }

    @Test(expected = IllegalArgumentException.class)
    public void firstOperatorIsNull() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        new UnionAll_Default(null, first.rowType(), new TestOperator(second), second.rowType(), openBoth());
    }

    @Test(expected = IllegalArgumentException.class)
    public void firstRowTypeIsNull() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        new UnionAll_Default(new TestOperator(first), null, new TestOperator(second), second.rowType(), openBoth());
    }

    @Test(expected = IllegalArgumentException.class)
    public void secondOperatorIsNull() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        new UnionAll_Default(new TestOperator(first), first.rowType(), null, second.rowType(), openBoth());
    }

    @Test(expected = IllegalArgumentException.class)
    public void secondRowTypeIsNull() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        new UnionAll_Default(new TestOperator(first), first.rowType(), new TestOperator(second), null, openBoth());
    }

    /**
     * Tests what happens when one of the input streams outputs a rowType other than what we promised it would.
     * To make this test a bit more interesting, the outputted row is actually of the same shape as the expected
     * results: it just has a different rowTypeId.
     */
    @Test(expected = UnionAll_Default.WrongRowTypeException.class)
    public void inputsContainUnspecifiedRows() {
        Schema schema = newSchema();
        RowsBuilder first = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());
        RowsBuilder second = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar());

        RowsBuilder anotherStream = new RowsBuilder(schema, MNumeric.INT.instance(false), MString.varchar())
                .row(3, "three");
        first.rows().push(anotherStream.rows().pop());

        Operator union = union(first, second);
        OperatorTestHelper.execute(union);
    }

    private void check(Operator union, RowsBuilder expected) {
        final RowType outputRowType = union.rowType();
        checkRowTypes(expected.rowType(), outputRowType);

        OperatorTestHelper.check(union, expected.rows(), new OperatorTestHelper.RowCheck() {
            @Override
            public void check(Row row) {
                assertEquals("row types", outputRowType, row.rowType());
            }
        });
    }

    private void check(RowsBuilder rb1, RowsBuilder rb2, RowsBuilder expected) {
        check(union(rb1, rb2), expected);
    }

    private static void checkRowTypes(RowType expected, RowType actual) {
        assertEquals("number of fields", expected.nFields(), actual.nFields());
        for (int i=0; i < expected.nFields(); ++i) {
            assertEquals("field " + i, expected.typeAt(i), actual.typeAt(i));
        }
    }

    private Operator union(RowsBuilder rb1, RowsBuilder rb2) {
        return new UnionAll_Default(
                    new TestOperator(rb1),
                    rb1.rowType(),
                    new TestOperator(rb2),
                    rb2.rowType(),
                    openBoth()
            );
    }
}
