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
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.*;

public class QueryBindingsTest 
{
    @Test
    public void valueTest() {
        QueryBindings bindings = new SparseArrayQueryBindings();
        ValueSource value = new Value(MNumeric.INT.instance(false), 123);
        bindings.setValue(1, value);
        assertTrue(TClass.areEqual(value, bindings.getValue(1)));
    }

    @Test(expected=BindingNotSetException.class)
    public void unboundValueTest() {
        QueryBindings bindings = new SparseArrayQueryBindings();
        ValueSource value = new Value(MNumeric.INT.instance(false), 0);
        bindings.setValue(0, value);
        bindings.getValue(1);
    }

    @Test
    public void rowTest() {
        QueryBindings bindings = new SparseArrayQueryBindings();
        Deque<Row> rows = new RowsBuilder(MNumeric.INT.instance(false))
            .row(100)
            .row(101)
            .rows();
        for (Row row : rows) {
            bindings.setRow(1, row);
            assertEquals(row, bindings.getRow(1));
        }
    }

    @Test(expected=BindingNotSetException.class)
    public void unboundRowTest() {
        QueryBindings bindings = new SparseArrayQueryBindings();
        bindings.getRow(1);
    }

    @Test
    public void inheritanceTest() {
        QueryBindings parent = new SparseArrayQueryBindings();
        assertEquals(0, parent.getDepth());
        ValueSource value = new Value(MNumeric.INT.instance(false), 1);
        parent.setValue(0, value);
        QueryBindings child = parent.createBindings();
        assertEquals(1, child.getDepth());
        assertTrue(parent.isAncestor(parent));
        assertTrue(child.isAncestor(parent));
        assertFalse(parent.isAncestor(child));
        Deque<Row> rows = new RowsBuilder(MNumeric.INT.instance(false))
            .row(100)
            .row(101)
            .rows();
        for (Row row : rows) {
            child.setRow(1, row);
            assertEquals(row, child.getRow(1));
        }
        assertTrue(TClass.areEqual(value, child.getValue(0)));
        try {
            parent.getRow(1);
            fail();
        }
        catch (BindingNotSetException ex) {
        }
    }

}
