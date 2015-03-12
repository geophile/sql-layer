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

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class Operator implements Plannable
{
    // Object interface

    @Override
    public String toString()
    {
        return getName();
    }

    // Operator interface

    @Override
    public String getName()
    {
        return getClass().getSimpleName();
    }

    // I'm not sure I like having this as part of the interface. On one hand, operators like Flatten create new
    // RowTypes and it's handy to get access to those new RowTypes. On the other hand, not all operators do this,
    // and it's conceivable we'll have to invent an operator for which this doesn't make sense, e.g., it creates
    // multiple RowTypes.
    public RowType rowType()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Find the derived types created by this operator and its inputs. A <i>derived type</i> is a type generated
     * by an operator, and as such, does not correspond to an AIS Table or Index.
     * @param derivedTypes Derived types created by this operator or input operators are added to derivedTypes.
     */
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
    }

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.emptyList();
    }

    protected abstract Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor);

    @Override
    public String describePlan()
    {
        return toString();
    }

    @Override
    public final String describePlan(Operator inputOperator)
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(inputOperator.describePlan());
        buffer.append(NL);
        buffer.append(toString());
        return buffer.toString();
    }

    // For use by subclasses

    protected int ordinal(Table table)
    {
        return table.getOrdinal();
    }

    // Class state

    protected static final String NL = System.getProperty("line.separator");
    public static final InOutTap OPERATOR_TAP = Tap.createRecursiveTimer("operator: root");
}
