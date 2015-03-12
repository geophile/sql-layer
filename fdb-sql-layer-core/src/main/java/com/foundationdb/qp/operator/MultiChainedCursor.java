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

/**
 * The third of the three complete implementations of the CursorBase
 * interface. 
 * 
 * The MultiChainedCursor works in the middle of the operator tree, taking
 * input from two input operators and passing the resulting aggregation
 * through next()
 * 
 * The MultiChainedCursor assumes nothing about the QueryBindings, 
 * in general does not use them, and only passes them along to either
 * parent or child. 
 * 
 * @See LeafCursor
 * @see ChainedCursor
 *
 * Used by:
 * @see Except_Ordered$Execution
 * @see HKeyUnion_Ordered$Execution
 * @see Intersect_Ordered$Execution
 * @see Union_Ordered$Execution
 * @see UnionAll_Default$Execution
 */
public abstract class MultiChainedCursor extends  OperatorCursor {

    protected Cursor leftInput;
    protected Cursor rightInput;
    protected final QueryBindingsCursor bindingsCursor;
    
    protected MultiChainedCursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        super(context);
        MultipleQueryBindingsCursor multiple = new MultipleQueryBindingsCursor(bindingsCursor);
        this.bindingsCursor = multiple;
        this.leftInput = left().cursor(context, multiple.newCursor());
        this.rightInput = right().cursor(context, multiple.newCursor());
    }
    
    protected abstract Operator left();
    protected abstract Operator right();

    @Override
    public void open() {
        CursorLifecycle.checkClosed(leftInput);
        CursorLifecycle.checkClosed(rightInput);
        leftInput.open();
        rightInput.open();
        super.open();
    }

    @Override
    public void close() {
        try {
            if (CURSOR_LIFECYCLE_ENABLED) {
                CursorLifecycle.checkIdleOrActive(leftInput);
                CursorLifecycle.checkIdleOrActive(rightInput);
            }
            leftInput.close();
            rightInput.close();
        } finally {
            super.close();
        }
    }

    @Override
    public void openBindings() {
        bindingsCursor.openBindings();
        leftInput.openBindings();
        rightInput.openBindings();
    }

    @Override
    public QueryBindings nextBindings() {
        CursorLifecycle.checkClosed(this);
        QueryBindings bindings = bindingsCursor.nextBindings();
        QueryBindings other = leftInput.nextBindings();
        assert (bindings == other);
        other = rightInput.nextBindings();
        assert (bindings == other);
        return bindings;
    }

    @Override
    public void closeBindings() {
        bindingsCursor.closeBindings();
        leftInput.closeBindings();
        rightInput.closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings bindings) {
        CursorLifecycle.checkClosed(this);
        //close();                // In case override maintains some additional state.
        leftInput.cancelBindings(bindings);
        rightInput.cancelBindings(bindings);
        bindingsCursor.cancelBindings(bindings);
    }
}
