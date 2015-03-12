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

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.ais.model.HKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;

public class FlattenedRowType extends CompoundRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("flatten(%s, %s)", first(), second());
    }

    // RowType interface

    @Override
    public HKey hKey()
    {
        return second().hKey();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.PARENT_TYPE, first().getExplainer(context));
        explainer.addAttribute(Label.CHILD_TYPE, second().getExplainer(context));
        return explainer;
    }

    // FlattenedRowType interface

    public RowType parentType()
    {
        return first();
    }

    public RowType childType()
    {
        return second();
    }

    public FlattenedRowType(Schema schema, int typeId, RowType parent, RowType child)
    {
        super(schema, typeId, parent, child);
        // re-replace the type composition with the single branch type
        List<Table> parentAndChildTables = new ArrayList<>(parent.typeComposition().tables());
        parentAndChildTables.addAll(child.typeComposition().tables());
        typeComposition(new SingleBranchTypeComposition(this, parentAndChildTables));
        
    }
}
