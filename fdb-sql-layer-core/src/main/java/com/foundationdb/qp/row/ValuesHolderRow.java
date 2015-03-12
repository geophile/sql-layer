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


import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.Value;

import java.io.Serializable;
import java.util.List;

public class ValuesHolderRow extends AbstractValuesHolderRow implements Serializable {

    // ValuesHolderRow interface -- mostly just promoting visiblity

    private static final long serialVersionUID = 1L;

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public Value valueAt(int index) {
        return super.valueAt(index);
    }

    public List<Value> values() {
        return super.values;
    }

    public ValuesHolderRow(RowType rowType) {
        super(rowType, true);
    }

    public ValuesHolderRow(RowType rowType, List<Value> values) {
        super(rowType, values);
    }
    
    public ValuesHolderRow(RowType rowType, Value... values) {
        super(rowType, values);
    }
    
    public ValuesHolderRow(RowType rowType, Object... objects) {
        super(rowType, objects);
    }
}
