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
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 <h1>Overview</h1>

 Select_HKeyOrdered passes on selected rows from the input stream to the output stream. A row is subject to elimination
 if and only if it's type is a specified type (predicateType), or a descendent of this type.
 
 <h1>Arguments</h1>

 <li><b>Operator inputOperator:</b> Operator providing the input stream.
 <li><b>RowType predicateRowType:</b> Type of row to which the selection predicate is applied.
 <li><b>Expression predicate:</b> Selection predicate.
 
 <h1>Behavior</h1>
 
 The handling of a row depends on its RowType:
 
 If the row's type matches predicateRowType: The predicate is evaluated. The row is written to the output stream
 if and only if the predicate evaluates to true. 
 
 If the row's type is a descendent type of predicateRowType: The row is written to the output stream if and only if
 the predicate evaluated to true for the ancestor of type predicateRowType. (E.g., if a Customer is rejected,
 then all of its Orders and Items will be rejected too.)
 
 All other rows are written to the output stream unconditionally.

 <h1>Output</h1>

 A subset of the rows from the input stream.

 <h1>Assumptions</h1>

 Input is hkey-ordered with respect to predicateRowType. E.g., in a COI schema, with prediateRowType = Order, 
 Orders and Items are assumed to be in hkey-order. The order of one Order relative to another is not significant, nor
 is the order of Customers.

 <h1>Performance</h1>

 Project_Default does no IO. For each input row, the type is checked and each output field is computed.

 <h1>Memory Requirements</h1>

 One row of type predicateRowType.
 
 */

class Select_HKeyOrdered extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s, %s)", getClass().getSimpleName(), predicateRowType, 
                pPredicate.toString());
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

    // Select_HKeyOrdered interface
    public Select_HKeyOrdered(Operator inputOperator, RowType predicateRowType, TPreparedExpression pPredicate)
    {
        ArgumentValidation.notNull("predicateRowType", predicateRowType);
        this.inputOperator = inputOperator;
        this.predicateRowType = predicateRowType;
        this.groupScanInput = !(predicateRowType instanceof IndexRowType);
        this.pPredicate = pPredicate;
        
        ArgumentValidation.notNull("predicate", pPredicate);
        if (pPredicate.resultType().typeClass() != AkBool.INSTANCE)
            throw new IllegalArgumentException("predicate must return type " + AkBool.INSTANCE);
    }

    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Select_HKeyOrdered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Select_HKeyOrdered next");
    private static final Logger LOG = LoggerFactory.getLogger(Select_HKeyOrdered.class);

    // Object state

    private final Operator inputOperator;
    private final RowType predicateRowType;
    private final boolean groupScanInput;
    private final TPreparedExpression pPredicate;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes att = new Attributes();
        att.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        att.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        att.put(Label.PREDICATE, pPredicate.getExplainer(context));
        return new CompoundExplainer(Type.SELECT_HKEY, att);
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
                pEvaluation.with(context);
                pEvaluation.with(bindings);
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
                Row row = null;
                Row inputRow = input.next();
                while (row == null && inputRow != null) {
                    if (inputRow.rowType() == predicateRowType) {
                        pEvaluation.with(inputRow);
                        pEvaluation.evaluate();
                        if (pEvaluation.resultValue().getBoolean(false)) {
                            // New row of predicateRowType
                            if (groupScanInput) {
                                selectedRow = inputRow;
                            }
                            row = inputRow;
                        }
                    } else if (predicateRowType.ancestorOf(inputRow.rowType())) {
                        // Row's type is a descendent of predicateRowType.
                        if (selectedRow != null && selectedRow.ancestorOf(inputRow)) {
                            row = inputRow;
                        } else {
                            selectedRow = null;
                        }
                    } else {
                        row = inputRow;
                    }
                    if (row == null) {
                        inputRow = input.next();
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Select_HKeyOrdered: yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close()
        {
            super.close();
            selectedRow = null;
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
            this.pEvaluation = pPredicate.build();
        }

        // Object state

        private Row selectedRow; // The last input row with type = predicateRowType.
        private final TEvaluatableExpression pEvaluation;
    }
}
