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

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;

public class SortOperatorExplainer extends CompoundExplainer
{
    public SortOperatorExplainer (String name, API.SortOption sortOption, RowType sortType, Operator inputOp, API.Ordering ordering, ExplainContext context)
    {
        super(Type.SORT, buildMap(name, sortOption, sortType, inputOp, ordering, context));
    }
    
    private static Attributes buildMap (String name, API.SortOption sortOption, RowType sortType, Operator inputOp, API.Ordering ordering, ExplainContext context)
    {
        Attributes map = new Attributes();
        
        map.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        map.put(Label.SORT_OPTION, PrimitiveExplainer.getInstance(sortOption.name()));
        map.put(Label.ROWTYPE, sortType.getExplainer(context));
        map.put(Label.INPUT_OPERATOR, inputOp.getExplainer(context));
        for (int i = 0; i < ordering.sortColumns(); i++)
        {
            map.put(Label.EXPRESSIONS, ordering.expression(i).getExplainer(context));
            map.put(Label.ORDERING, PrimitiveExplainer.getInstance(ordering.ascending(i) ? "ASC" : "DESC"));
        }
        
        return map;
    }
}
