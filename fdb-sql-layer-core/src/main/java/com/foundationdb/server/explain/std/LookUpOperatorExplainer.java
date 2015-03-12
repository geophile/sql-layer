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

public class LookUpOperatorExplainer extends CompoundExplainer
{
    public LookUpOperatorExplainer (String name, Attributes atts, RowType iRowType, boolean keepInput, Operator inputOp, ExplainContext context)
    {
        super(Type.LOOKUP_OPERATOR, buildAtts(name, atts, iRowType, keepInput, inputOp, context));
    }
    
    private static Attributes buildAtts (String name, Attributes atts, RowType iRowType, boolean keepInput, Operator inputOp, ExplainContext context)
    {
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        atts.put(Label.ROWTYPE, iRowType.getExplainer(context));
        atts.put(Label.INPUT_TYPE, iRowType.getExplainer(context));
        atts.put(Label.INPUT_PRESERVATION, PrimitiveExplainer.getInstance((keepInput ? "KEEP_INPUT" : "DISCARD_INPUT")));
        if (null != inputOp)
            atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer(context));
        return atts;
    }
}
