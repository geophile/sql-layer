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
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.std.SortOperatorExplainer;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.qp.storeadapter.Sorter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 <h1>Overview</h1>

 Sort_General generates an output stream containing all the rows of the input stream, sorted according to an
 ordering specification. The "General" in the name refers to the flexible implementation which is provided by
 the underlying {@link StoreAdapter}.

 <h1>Arguments</h1>

 <li><b>Operator inputOperator:</b> Operator providing the input stream.
 <li><b>RowType sortType:</b> Type of rows to be sorted.
 <li><b>API.Ordering ordering:</b> Specification of ordering, comprising a list of expressions and ascending/descending
 specifications.
 <li><b>API.SortOption sortOption:</b> Specifies whether duplicates should be kept (PRESERVE_DUPLICATES) or eliminated
 (SUPPRESS_DUPLICATES)

 <h1>Behavior</h1>

 Refer to specific implementations of {@link Sorter} for details.

 <h1>Output</h1>

 The rows of the input stream, sorted according to the ordering specification. Duplicates are eliminated if
 and only if the sortOption is SUPPRESS_DUPLICATES.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Refer to specific implementations of {@link Sorter} for details.

 <h1>Memory Requirements</h1>

 Refer to specific implementations of {@link Sorter} for details.

 */
class Sort_General extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        if (sortOption == API.SortOption.PRESERVE_DUPLICATES)
            return String.format("%s(%s)", getClass().getSimpleName(), sortType);
        else
            return String.format("%s(%s, %s)", getClass().getSimpleName(), sortType, sortOption.name());
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
        return sortType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(sortType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Sort_General interface

    public Sort_General(Operator inputOperator,
                        RowType sortType,
                        API.Ordering ordering,
                        API.SortOption sortOption)
    {
        ArgumentValidation.notNull("sortType", sortType);
        ArgumentValidation.isGT("ordering.columns()", ordering.sortColumns(), 0);
        this.inputOperator = inputOperator;
        this.sortType = sortType;
        this.ordering = ordering;
        this.sortOption = sortOption;
    }
    
    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_General open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_General next");
    private static final InOutTap TAP_LOAD = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_General load");
    private static final Logger LOG = LoggerFactory.getLogger(Sort_General.class);

    // Object state

    private final Operator inputOperator;
    private final RowType sortType;
    private final API.Ordering ordering;
    private final API.SortOption sortOption;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new SortOperatorExplainer(getName(), sortOption, sortType, inputOperator, ordering, context);
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
                output = new SorterToCursorAdapter(adapter(), context, bindings, input, sortType, ordering, sortOption, TAP_LOAD);
                output.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            Row row = null;
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                checkQueryCancelation();
                row = output.next();
                if (row == null) {
                    setIdle();
                }
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
            if (LOG_EXECUTION) {
                LOG.debug("Sort_General: yield {}", row);
            }
            return row;
        }

        @Override
        public void close()
        {
            super.close();
            output.close();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
        }

        // Object state

        private RowCursor output;
    }
}
