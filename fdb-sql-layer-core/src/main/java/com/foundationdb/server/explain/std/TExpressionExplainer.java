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

import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import java.util.Arrays;
import java.util.List;

public class TExpressionExplainer extends CompoundExplainer
{  
    public TExpressionExplainer(Type type, String name, ExplainContext context, List<? extends TPreparedExpression> exs)
    {
        super(checkType(type), buildMap(name, context, exs));
    }
     
    public TExpressionExplainer(Type type, String name, ExplainContext context, TPreparedExpression ... operand)
    {
        this(type, name, context, Arrays.asList(operand));
    }
        
    private static Attributes buildMap(String name, ExplainContext context, List<? extends TPreparedExpression> exs)
    {
        Attributes states = new Attributes();
        states.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        if (exs != null)
            for (TPreparedExpression ex : exs)
                states.put(Label.OPERAND, ex.getExplainer(context));
        return states;
    }
    
    private static Type checkType(Type type)
    {
        if (type.generalType() != Type.GeneralType.EXPRESSION)
            throw new IllegalArgumentException("Expected sub-category of Type.GeneralType.EXPRESSION but got " + type);
        return type;
    }
}
