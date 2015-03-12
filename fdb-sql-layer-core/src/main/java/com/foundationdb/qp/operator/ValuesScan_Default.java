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

import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**

 <h1>Overview</h1>

 ValuesScan_Default is an in-memory collection of identical rows used
 as a source operator.

 <h1>Arguments</h1>

 <ul>

 <li><b>List<ExpressionRow> rows:</b> the list of ExpressionRows to be
 returned by the cursor in order

 <h1>Behaviour </h1>

 The rows are returned in the order they are present in the list.

 <h1>Output</h1>

 Rows as given

 <h1>Assumptions</h1>

 None

 <h1>Performance</h1>

 No I/O cost, as the list is maintained in memory.

 <h1>Memory Requirements</h1>

 Memory requirement is for the number of rows stored in the list
 supplied. There are no memory requirement beyond that.

 */

public class ValuesScan_Default extends Operator
{

    // Operator interface

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, bindingsCursor, rows);
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName()  + rows;
    }

    public ValuesScan_Default (Collection<? extends BindableRow> bindableRows, RowType rowType) {
        this.rows = new ArrayList<>(bindableRows);
        this.rowType = rowType;
    }

    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: ValuesScan_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: ValuesScan_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(ValuesScan_Default.class);

    // Object state
    
    private final Collection<? extends BindableRow> rows;
    private final RowType rowType;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes att = new Attributes();
        
        att.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        for (BindableRow row : rows)
        {
            att.put(Label.EXPRESSIONS, row.getExplainer(context));
        }
        
        return new CompoundExplainer(Type.SCAN_OPERATOR, att);
    }
    
    private static class Execution extends LeafCursor
    {
        private final Collection<? extends BindableRow> rows;
        private Iterator<? extends BindableRow> iter;

        public Execution (QueryContext context, QueryBindingsCursor bindingsCursor, Collection<? extends BindableRow> rows) {
            super(context, bindingsCursor);
            this.rows = rows;
        }

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                super.open();
                iter = rows.iterator();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public void close() {
            super.close();
            iter = null;
        }

        @Override
        public Row next() {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                Row output;
                if (iter != null && iter.hasNext()) {
                    output = iter.next().bind(context, bindings);
                } else {
                    setIdle();
                    output = null;
                }
                if (LOG_EXECUTION) {
                    LOG.debug("ValuesScan_Default: yield {}", output);
                }
                return output;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

    }
}
