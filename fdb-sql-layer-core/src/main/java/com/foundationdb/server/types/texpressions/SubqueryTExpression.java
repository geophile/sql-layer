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
package com.foundationdb.server.types.texpressions;

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.util.ArgumentValidation;

abstract class SubqueryTExpression implements TPreparedExpression {
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return new TPreptimeValue(resultType());
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes states = new Attributes();
        states.put(Label.OPERAND, subquery.getExplainer(context)); 
        states.put(Label.OUTER_TYPE, outerRowType == null
                                        ? PrimitiveExplainer.getInstance("<EMPTY>")  
                                        :outerRowType.getExplainer(context));
        states.put(Label.INNER_TYPE, innerRowType == null
                                        ? PrimitiveExplainer.getInstance("<EMPTY>") 
                                        : innerRowType.getExplainer(context));
        states.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(bindingPosition));
        return new CompoundExplainer(Type.SUBQUERY, states);
    }
    
    @Override
    public boolean isLiteral() {
        return false;
    }

    // for use by subclasses

    protected final Operator subquery() {
        return subquery;
    }
    protected final RowType outerRowType() {
        return outerRowType;
    }
    protected final RowType innerRowType() {
        return innerRowType;
    }
    protected final int bindingPosition() {
        return bindingPosition;
    }

    SubqueryTExpression(Operator subquery, RowType outerRowType,
                        RowType innerRowType, int bindingPosition) {
        ArgumentValidation.notNull("subquery", subquery);
        ArgumentValidation.isGTE("binding position", bindingPosition, 0);
        this.subquery = subquery;
        this.outerRowType = outerRowType;
        this.innerRowType = innerRowType;
        this.bindingPosition = bindingPosition;
    }

    private final Operator subquery;
    private final RowType outerRowType;
    private final RowType innerRowType;
    private final int bindingPosition;
}
