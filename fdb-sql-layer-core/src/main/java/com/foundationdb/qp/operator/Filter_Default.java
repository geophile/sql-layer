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
import com.foundationdb.server.explain.std.FilterExplainer;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**

 <h1>Overview</h1>

 Extract_Default filters the input stream, keeping rows that match or are descendents of a given type, and discarding others.

 <h1>Arguments</h1>

 <ul>
 <li><b>Collection<RowType> extractTypes:</b> Specifies types of rows to be passed on to the output stream. 
</ul>
 
 <h1>Behavior</h1>

 A row is kept if its type matches one of the extractTypes, or is a descendent of one of these types. Other rows are discarded.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Extract_Default does no IO. For each input row, the type is checked and the row is either kept (written to the output stream) or discarded.

 <h1>Memory Requirements</h1>

 None.

 
 */

class Filter_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        TreeSet<String> keepTypesStrings = new TreeSet<>();
        for (RowType keepType : keepTypes) {
            keepTypesStrings.add(String.valueOf(keepType));
        }
        return String.format("%s(%s)", getClass().getSimpleName(), keepTypesStrings);
    }

    // Operator interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

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
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Filter_Default interface

    public Filter_Default(Operator inputOperator, Collection<? extends RowType> keepTypes)
    {
        ArgumentValidation.notEmpty("keepTypes", keepTypes);
        this.inputOperator = inputOperator;
        this.keepTypes = new HashSet<>(keepTypes);
    }
    
    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Filter_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Filter_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Filter_Default.class);

    // Object state

    private final Operator inputOperator;
    private final Set<RowType> keepTypes;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new FilterExplainer(getName(), keepTypes, inputOperator, context);
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
                Row row;
                do {
                    row = input.next();
                    if (row == null) {
                        setIdle();
                    } else if (!keepTypes.contains(row.rowType())) {
                        row = null;
                    }
                } while (row == null && isActive());
                if (LOG_EXECUTION) {
                    LOG.debug("Filter_Default: yield {}", row);
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

    }
}
