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
import com.foundationdb.server.api.dml.ColumnSelector;

/**
 * The first of the three complete implementations of the CursorBase
 * interface. 
 * 
 * The ChainedCursor works in the middle of the operator tree, taking
 * input from the input operator and passing along rows to the caller to next().
 * 
 * The ChainedCursor assumes nothing about the QueryBindings, 
 * in general does not use them, and only passes them along to either
 * parent or child. 
 * 
 * @See LeafCursor
 * @see MultiChainedCursor
 *
 * Used by:
 * @see Aggregate_Partial
 * @see BranchLookup_Default
 * @see Buffer_Default
 * @see Count_Default
 * @see Delete_Returning
 * @see Distinct_Partial
 * @see Filter_Default
 * @see Flatten_HKeyOrdered
 * @see GroupLookup_Default (non-lookahead)
 * @see IfEmpty_Default
 * @see Insert_Returning
 * @see Limit_Default
 * @see Product_Nested
 * @see Project_Default
 * @see Select_BloomFilter (non-lookahead)
 * @see Select_HKeyOrdered
 * @see Sort_General
 * @see Sort_InsertLimited
 * @see Update_Returning
 * @see Using_BloomFilter
 * @see Using_HashTable
 * 
 */
public class ChainedCursor extends OperatorCursor
{
    protected final Cursor input;
    protected QueryBindings bindings;
    
    protected ChainedCursor(QueryContext context, Cursor input) {
        super(context);
        this.input = input;
        if (input == null) {
            throw new NullPointerException("Input to ChainedCursor");
        }
    }

    public Cursor getInput() {
        return input;
    }

    @Override
    public void open() {
        super.open();
        input.open();
    }
    @Override
    public Row next() {
        return input.next();
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector) {
        if (CURSOR_LIFECYCLE_ENABLED) {
            CursorLifecycle.checkIdleOrActive(this);
        }
        input.jump(row, columnSelector);
        state = CursorLifecycle.CursorState.ACTIVE;
    }
    
    @Override
    public void close() {
        try {
            input.close();
        } finally {
            super.close();
        }
    }

    @Override
    public void openBindings() {
        input.openBindings();
    }

    @Override
    public QueryBindings nextBindings() {
        CursorLifecycle.checkClosed(this);
        bindings = input.nextBindings();
        return bindings;
    }

    @Override
    public void closeBindings() {
        input.closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings ancestor) {
        CursorLifecycle.checkClosed(this);
        input.cancelBindings(ancestor);
    }
}
