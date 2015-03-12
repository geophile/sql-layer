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

import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.List;

public class ProjectedRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        List<?> projectionsToString = tExprs;
        return String.format("project(%s)", projectionsToString);
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return tInstances.size();
    }

    @Override
    public TInstance typeAt(int index) {
        return tInstances.get(index);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        for (TPreparedExpression expr : tExprs) {
            explainer.addAttribute(Label.EXPRESSIONS, expr.getExplainer(context));
        }
        return explainer;
    }

    // ProjectedRowType interface

    public ProjectedRowType(Schema schema, int typeId, List<? extends TPreparedExpression> tExpr)
    {
        super(schema, typeId);
        ArgumentValidation.notNull("tExpressions", tExpr);
        this.tExprs = tExpr;
        this.tInstances = new ArrayList<>(tExpr.size());
        for (TPreparedExpression expr : tExpr)
            tInstances.add(expr.resultType());
    }

    protected List<? extends TPreparedExpression> getExpressions() {
        return tExprs;
    }

    // Object state

    private final List<? extends TPreparedExpression> tExprs;
    private final List<TInstance> tInstances;
}
