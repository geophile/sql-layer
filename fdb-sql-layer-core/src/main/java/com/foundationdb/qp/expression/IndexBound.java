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
package com.foundationdb.qp.expression;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.types.value.ValueRecord;

public class IndexBound
{
    public String toString()
    {
        return String.valueOf(unboundExpressions);
    }

    public ValueRecord boundExpressions(QueryContext context, QueryBindings bindings)
    {
        return unboundExpressions.get(context, bindings);
    }

    public ColumnSelector columnSelector()
    {
        return columnSelector;
    }

    public CompoundExplainer getExplainer(ExplainContext context) {
        return unboundExpressions.getExplainer(context);
    }

    public IndexBound(ValueRecord row, ColumnSelector columnSelector)
    {
        this(new PreBoundExpressions(row), columnSelector);
    }

    public IndexBound(UnboundExpressions unboundExpressions, ColumnSelector columnSelector)
    {
        this.unboundExpressions = unboundExpressions;
        this.columnSelector = columnSelector;
    }
    
    public boolean isLiteral(int index) {
        return unboundExpressions.isLiteral(index);
    }

    // Object state

    private final UnboundExpressions unboundExpressions;
    private final ColumnSelector columnSelector;

    // nested classes

    private static class PreBoundExpressions implements UnboundExpressions {

        @Override
        public ValueRecord get(QueryContext context, QueryBindings bindings) {
            return expressions;
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return String.valueOf(expressions);
        }
        
        @Override 
        public boolean isLiteral(int index) {
            // Because this is built off a row queried from the database, 
            // none of the values can ever be a literal null.
            return false;
        }

        public PreBoundExpressions(ValueRecord expressions) {
            this.expressions = expressions;
        }

        private final ValueRecord expressions;
    }
}
