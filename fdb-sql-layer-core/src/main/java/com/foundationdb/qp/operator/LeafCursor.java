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
 * LeafCursor handles the cursor processing for the Leaf operators.
 * Unlike the ChainedCursor or DualChainedCursors, the LeafCursor
 * isn't reading data from another operator cursor, but is reading it
 * from an underlying Row source. Usually this is a adapter row, or 
 * row collection. 
 * 
 * @see ChainedCursor
 * @see MultiChainedCursor
 * 
 * Used By
 * @see AncestorLookup_Nested$Execution (non-lookahead)
 * @see BranchLookup_Nested$Execution
 * @see Count_TableStatus$Execution
 * @see GroupScan_Default$Execution
 * @see HashTableLookup_Default$Execution
 * @see HKeyRow_Default$Execution
 * @see IndexScan_Default$Execution
 * @see ValuesScan_Default$Execution
 * 
 * @see com.foundationdb.server.service.text.IndexScan_FullText$Execution
 * @see com.foundationdb.server.test.it.qp.QueryTimeoutIT$DoNothingForever$Execution
 */
public class LeafCursor extends OperatorCursor
{
    protected final QueryBindingsCursor bindingsCursor;
    protected QueryBindings bindings;

    protected LeafCursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        super(context);
        this.bindingsCursor = bindingsCursor;
    }

    @Override
    public void openBindings() {
        bindingsCursor.openBindings();
    }

    @Override
    public QueryBindings nextBindings() {
        CursorLifecycle.checkClosed(this);
        bindings = bindingsCursor.nextBindings();
        return bindings;
    }

    @Override
    public void closeBindings() {
        bindingsCursor.closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings bindings) {
        CursorLifecycle.checkClosed(this);
        bindingsCursor.cancelBindings(bindings);
    }
}
