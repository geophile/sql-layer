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
package com.foundationdb.qp.rowtype;

import com.foundationdb.ais.model.Table;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.types.TInstance;

import java.util.HashSet;
import java.util.Set;

public class ProductRowType extends CompoundRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("product(%s: %s x %s)", branchType, first(), second());
    }

    // RowType interface

    @Override
    public TInstance typeAt(int index) {
        if (index < first().nFields())
            return first().typeAt(index);
        return second().typeAt(index - first().nFields() + branchType.nFields());
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.LEFT_TYPE, first().getExplainer(context));
        explainer.addAttribute(Label.RIGHT_TYPE, second().getExplainer(context));
        return explainer;
    }

    // ProductRowType interface

    public RowType branchType()
    {
        return branchType;
    }

    public RowType leftType()
    {
        return first();
    }

    public RowType rightType()
    {
        return second();
    }

    public ProductRowType(Schema schema,
                          int typeId, 
                          RowType leftType, 
                          TableRowType branchType,
                          RowType rightType)
    {
        super(schema, typeId, leftType, rightType);
        this.branchType =
            branchType == null
            ? leafmostCommonType(leftType, rightType)
            : branchType;
        this.nFields = leftType.nFields() + rightType.nFields() - this.branchType.nFields();
    }

    // For use by this class

    private static RowType leafmostCommonType(RowType leftType, RowType rightType)
    {
        Set<Table> common = new HashSet<>(leftType.typeComposition().tables());
        common.retainAll(rightType.typeComposition().tables());
        Table leafmostCommon = null;
        for (Table table : common) {
            if (leafmostCommon == null || table.getDepth() > leafmostCommon.getDepth()) {
                leafmostCommon = table;
            }
        }
        assert leafmostCommon != null : String.format("leftType: %s, rightType: %s", leftType, rightType);
        return leftType.schema().tableRowType(leafmostCommon);
    }

    // Object state

    private final RowType branchType;
}
