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
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.std.DUIOperatorExplainer;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

<h1>Overview</h1>

Provides row update functionality.

<h1>Arguments</h1>

<ul>

<li><b>PhysicalOperator inputOperator:</b> Provides rows to be updated

<li><b>UpdateFunction updateFunction:</b> specifies which rows are to be updated, and how

</ul>

<h1>Behavior</h1>

For each row from the input operator's cursor, it invokes <i>updateFunction.evaluate</i> to
get the new version of the row. It then performs the update of the row in the table.
In praactice, this is currently done via {@link StoreAdapter#updateRow}.

The updated row is then returned to the caller as with other operators. 

<h1>Output</h1>

The row as modified by the updateFunction().

<h1>Assumptions</h1>

Selected rows must have a <i>RowType</i> such
that <i>rowType.hasTable() == true</i>.

<h1>Performance</h1>

Updating rows may be slow, especially since indexes are also
updated. There are several random-access reads and writes involved,
which depend on the indexes defined for that row type.

There are potentially ways to optimize this, if we can
push <i>WHERE</i> clauses down; this would mean we could update some
indexes as a batch operation, rather than one at a time. This would
require changes to the API, and is not currently a priority.

<h1>Memory Requirements</h1>

Each <i>UpdateFunction.evaluate</i> method may generate a
new <i>Row</i>.

*/

public class Update_Returning extends Operator {

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        atts.put(Label.EXTRA_TAG, PrimitiveExplainer.getInstance(updateFunction.toString()));
        return new DUIOperatorExplainer(getName(), atts, inputOperator, context);
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }
    
    public Update_Returning (Operator inputOperator, UpdateFunction updateFunction) {
        ArgumentValidation.notNull("update lambda", updateFunction);
        
        this.inputOperator = inputOperator;
        this.updateFunction = updateFunction;
    }

    @Override
    public String toString() {
        return String.format("%s(%s -> %s)", getName(), inputOperator, updateFunction);
    }

    private final Operator inputOperator;
    private final UpdateFunction updateFunction;
    // Class state
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: UpdateReturning open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: UpdateReturning next");
    private static final Logger LOG = LoggerFactory.getLogger(Update_Returning.class);

    // Inner classes
    private class Execution extends ChainedCursor
    {

        // Cursor interface
    
        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
            } finally {
                TAP_OPEN.out();
            }
        }
    
        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                checkQueryCancelation();
                
                Row inputRow;
                Row newRow = null;
                if ((inputRow = input.next()) != null) {
                    newRow = updateFunction.evaluate(inputRow, context, bindings);
                    adapter().updateRow(inputRow, newRow);
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Update_Returning: updating {} to {}", inputRow, newRow);
                }
                return newRow; 
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }
        // Execution interface
    
        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
        }
    
        // Object state
    }

}
