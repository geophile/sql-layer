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
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.LookUpOperatorExplainer;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**

 <h1>Overview</h1>

 EmitBoundRow_Nested recovers a row (or subrow) from a nested loop.

 When an <code>UPDATE</code> statement involves complex joins, the row
 to be updated is no longer an immediate input.

 <h1>Arguments</h1>

 <ul>

 <li><b>RowType inputRowType:</b> The input row that triggers the lookup.

 <li><b>RowType outputRowType:</b> The desired row type.

 <li><b>RowType boundRowType:</b> The type in the bindings.

 <li><b>int bindingPosition:</b> Indicates target row's position in the query context.

 </ul>

 <h1>Behavior</h1>
 
 The outer row is fetched from the query context for every inner row. No database access is required.

 <h1>Output</h1>

 The bound row (or a subrow).

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

  No datbase access.

 <h1>Memory Requirements</h1>

  No storage of its own.

 */

class EmitBoundRow_Nested extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), inputRowType, outputRowType);
    }

    // Operator interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // EmitBoundRow_Nested interface

    public EmitBoundRow_Nested(Operator inputOperator,
                            RowType inputRowType,
                            RowType outputRowType,
                            RowType boundRowType,
                            int bindingPosition)
    {
        validateArguments(inputRowType, outputRowType, boundRowType, bindingPosition); 
        this.inputOperator = inputOperator;
        this.inputRowType = inputRowType;
        this.outputRowType = outputRowType;
        this.boundRowType = boundRowType;
        this.bindingPosition = bindingPosition;
    }

    // For use by this class

    private void validateArguments(RowType inputRowType,
                                   RowType outputRowType,
                                   RowType boundRowType,
                                   int bindingPosition)
    {
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notNull("outputRowType", outputRowType);
        ArgumentValidation.notNull("boundRowType", boundRowType);
        ArgumentValidation.isTrue("bindingPosition >= 0", bindingPosition >= 0);
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(EmitBoundRow_Nested.class);
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: EmitBoundRow_Nested open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: EmitBoundRow_Nested next");

    // Object state

    private final Operator inputOperator;
    private final RowType inputRowType, outputRowType, boundRowType;
    private final int bindingPosition;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(bindingPosition));
        atts.put(Label.OUTPUT_TYPE, outputRowType.getExplainer(context));
        return new LookUpOperatorExplainer(getName(), atts, inputRowType, false, inputOperator, context);
    }

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
                Row row = input.next();
                if (LOG_EXECUTION) {
                    LOG.debug("EmitBoundRow: {}", row == null ? null : row);
                }
                if (row == null) {
                    setIdle();
                }
                else  {
                    assert (row.rowType() == inputRowType);
                    Row rowFromBindings = bindings.getRow(bindingPosition);
                    assert (rowFromBindings.rowType() == boundRowType);
                    if (boundRowType == outputRowType) {
                        row = rowFromBindings;
                    }
                    else {
                        row = rowFromBindings.subRow(outputRowType);
                        assert (row != null) : rowFromBindings;
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("EmitBoundRow_Nested: yield {}", row);
                }
                return row;
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
    }
}
