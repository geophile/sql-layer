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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TimeOperator extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, bindingsCursor);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
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

    // TimeOperator interface

    public long elapsedNsec()
    {
        return elapsedNsec;
    }

    public TimeOperator(Operator inputOperator)
    {
        this.inputOperator = inputOperator;
    }

    // Object state

    private final Operator inputOperator;
    private long elapsedNsec = 0;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    // Inner classes

    private class Execution extends ChainedCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            super.open();
            long start = System.nanoTime();
            input.open();
            long stop = System.nanoTime();
            elapsedNsec += stop - start;
        }

        @Override
        public Row next()
        {
            long start = System.nanoTime();
            Row next = input.next();
            long stop = System.nanoTime();
            elapsedNsec += stop - start;
            return next;
        }

        @Override
        public void close()
        {
            try {
                long start = System.nanoTime();
                input.close();
                long stop = System.nanoTime();
                elapsedNsec += stop - start;
            } finally {
                super.close();
            }
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context, inputOperator.cursor(context, bindingsCursor));
        }
    }
}
