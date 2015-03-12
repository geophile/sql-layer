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
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.std.DistinctExplainer;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**

 <h1>Overview</h1>

 Distinct_Partial eliminates duplicate rows that are adjacent in the input stream.
 (A sufficient but not necessary condition for elimination of all duplicates is
 that the input is sorted by all columns.) 

 <h1>Arguments</h1>

 <ul>

 <li><b>Operator input:</b> the input operator

 <li><b>RowType distinctType:</b> Specifies the type of rows from the input stream.

 </ul>

 <h1>Behavior</h1>
 
 The RowType of each input row must match the specified distinctType.
 
 For each maximal subsequence of input rows that match in all columns, one row will be written to output. 

 <h1>Output</h1>

 A subset of the input rows, in which no two adjacent rows match in all columns.
 
 <h1>Assumptions</h1>

 The input type of every input row is the specified distinctType.

 <h1>Performance</h1>

 This operator performs no IO. For each row, there is a comparison of one or more column values to the columns
 of a stored row. An attempt is made to minimize the number of such comparisons, but it is possible to compare
 every column of every input row.

 <h1>Memory requirements</h1>

 No more than two rows at any time.

 */

class Distinct_Partial extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), distinctType);
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
        return distinctType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(distinctType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Distinct_Partial interface

    public Distinct_Partial(Operator inputOperator, RowType distinctType, List<AkCollator> collators)
    {
        ArgumentValidation.notNull("distinctType", distinctType);
        this.inputOperator = inputOperator;
        this.distinctType = distinctType;
        this.collators = collators;
    }

    // Class state
    
    private final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Distinct_Partial open");
    private final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Distinct_Partial next");
    private static final Logger LOG = LoggerFactory.getLogger(Distinct_Partial.class);

    // Object state

    private final Operator inputOperator;
    private final RowType distinctType;
    private final List<AkCollator> collators;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new DistinctExplainer(getName(), distinctType, inputOperator, context);
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
                nvalid = 0;
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
                checkQueryCancelation();
                Row row;
                while ((row = input.next()) != null) {
                    assert row.rowType() == distinctType : row;
                    if (isDistinctP(row)) break;
                }
                if (row == null) {
                    setIdle();
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Distinct_Partial: yield {}", row);
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
            currentRow = null;
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);

            nfields = distinctType.nFields();
            currentValues = new Value[nfields];
            for (int i = 0; i < nfields; ++i) {
                currentValues[i] = new Value(distinctType.typeAt(i));
            }
        }

        private boolean isDistinctP(Row inputRow) {
            if ((nvalid == 0) && currentRow == null) {
                // Very first row.
                currentRow = inputRow;
                return true;
            }
            for (int i = 0; i < nfields; i++) {
                if (i == nvalid) {
                    assert currentRow != null;
                    ValueTargets.copyFrom(currentRow.value(i), currentValues[i]);
                    nvalid++;
                    if (nvalid == nfields)
                        // Once we have copies of all fields, don't need row any more.
                        currentRow = null;
                }
                ValueSource inputValue = inputRow.value(i);
                if (!TClass.areEqual(currentValues[i], inputValue)) {
                    ValueTargets.copyFrom(inputValue, currentValues[i]);
                    nvalid = i + 1;
                    if (i < nfields - 1)
                        // Might need later fields.
                        currentRow = inputRow;
                    return true;
                }
            }
            return false;
        }

        // Object state

        private Row currentRow;
        private final int nfields;
        // currentValues contains copies of the first nvalid of currentRow's fields,
        // filled as needed.
        private int nvalid;
        private final Value[] currentValues;
    }
}
