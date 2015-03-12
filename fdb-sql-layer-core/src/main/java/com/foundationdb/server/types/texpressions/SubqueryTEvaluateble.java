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
package com.foundationdb.server.types.texpressions;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

abstract class SubqueryTEvaluateble implements TEvaluatableExpression {

    @Override
    public ValueSource resultValue() {
        return value;
    }

    @Override
    public void evaluate() {
        bindings.setRow(bindingPosition, outerRow);
        if (cursor == null) {
            cursor = API.cursor(subquery, context, bindings);
        }
        cursor.openTopLevel();
        try {
            doEval(value);
        } finally {
            cursor.closeTopLevel();
        }
    }

    @Override
    public void with(Row row) {
        if (row.rowType() != outerRowType) {
            throw new IllegalArgumentException("wrong row type: " + outerRowType +
                    " != " + row.rowType());
        }
        outerRow = row;
    }

    @Override
    public void with(QueryContext context) {
        this.context = context;
        cursor = null;
    }

    @Override
    public void with(QueryBindings bindings) {
        this.bindings = bindings;
        cursor = null;
    }

    protected abstract void doEval(ValueTarget out);

    protected QueryContext queryContext() {
        return context;
    }

    protected QueryBindings queryBindings() {
        return bindings;
    }

    protected Row next() {
        Row row = cursor.next();
        if ((row != null) &&
                (row.rowType() != innerRowType)) {
            throw new IllegalArgumentException("wrong row type: " + innerRowType +
                    " != " + row.rowType());
        }
        return row;
    }

    SubqueryTEvaluateble(Operator subquery, RowType outerRowType, RowType innerRowType, int bindingPosition,
                         TInstance underlying)
    {
        this.subquery = subquery;
        this.outerRowType = outerRowType;
        this.innerRowType = innerRowType;
        this.bindingPosition = bindingPosition;
        this.value = new Value(underlying);
    }

    private final Operator subquery;
    private final RowType outerRowType;
    private final RowType innerRowType;
    private final int bindingPosition;
    private final Value value;
    private Cursor cursor;
    private QueryContext context;
    private QueryBindings bindings;
    private Row outerRow;
}
