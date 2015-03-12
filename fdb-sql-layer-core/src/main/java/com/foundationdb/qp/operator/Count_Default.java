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

import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.ValuesRowType;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.std.CountOperatorExplainer;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**

 <h1>Overview</h1>

 Count_Default counts the number of rows of a specified RowType.

 <h1>Arguments</h1>

 <ul>

 <li><b>RowType countType:</b> Type of rows to be counted.

 </ul>


 <h1>Behavior</h1>

 The input rows whose type matches the countType are counted.

 <h1>Output</h1>

 A single row containing the row count (type long).

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 This operator keeps no rows in memory.

 */

class Count_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), countType);
    }

    // Operator interface

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }

    @Override
    public RowType rowType()
    {
        return resultType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(resultType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Count_Default interface

    public Count_Default(Operator inputOperator, RowType countType)
    {
        ArgumentValidation.notNull("countType", countType);
        this.inputOperator = inputOperator;
        this.countType = countType;
        this.resultType = countType.schema().newValuesType(MNumeric.BIGINT.instance(false));
    }
    
    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Count_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Count_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Count_Default.class);

    // Object state

    private final Operator inputOperator;
    private final RowType countType;
    private final ValuesRowType resultType;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new CountOperatorExplainer(getName(), countType, resultType, inputOperator, context);
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
                count = 0;
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
                while ((row == null) && this.isActive()) {
                    row = input.next();
                    if (row == null) {
                        setIdle();
                        row = new ValuesHolderRow(resultType, new Value(MNumeric.BIGINT.instance(false), count));
                    } else if (row.rowType() == countType) {
                        row = null;
                        count++;
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Count_Default: yield {}", row);
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

        // Object state

        private long count;
    }
}
