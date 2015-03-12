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
package com.foundationdb.server.explain.std;

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;

public class NestedLoopsExplainer extends CompoundExplainer
{
    public NestedLoopsExplainer (String name, Operator innerOp, Operator outerOp, RowType innerType, RowType outerType, ExplainContext context)
    {
        super(Type.NESTED_LOOPS, buildMap(name, innerOp, outerOp, innerType, outerType, context));
    }
    
    private static Attributes buildMap (String name, Operator innerOp, Operator outerOp, RowType innerType, RowType outerType, ExplainContext context)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.INPUT_OPERATOR, outerOp.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, innerOp.getExplainer(context));
        if (innerType != null)
            atts.put(Label.INNER_TYPE, innerType.getExplainer(context));
        if (outerType != null)
            atts.put(Label.OUTER_TYPE, outerType.getExplainer(context));
        
        return atts;
    }
}
