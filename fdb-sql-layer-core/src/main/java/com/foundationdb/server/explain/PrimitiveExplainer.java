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
package com.foundationdb.server.explain;

import java.math.BigDecimal;
import java.math.BigInteger;

public class PrimitiveExplainer extends Explainer
{
    public static PrimitiveExplainer getInstance (String st)
    {
        return new PrimitiveExplainer(Type.STRING, st);
    }
    
    public static PrimitiveExplainer getInstance (double n)
    {
        return new PrimitiveExplainer(Type.FLOATING_POINT, n);
    }
    
    public static PrimitiveExplainer getInstance (long n)
    {
        return new PrimitiveExplainer(Type.EXACT_NUMERIC, n);
    }
    
    public static PrimitiveExplainer getInstance (boolean n)
    {
        return new PrimitiveExplainer(Type.BOOLEAN, n);
    }
    
    public static PrimitiveExplainer getInstance(BigInteger num)
    {
        return PrimitiveExplainer.getInstance(num.longValue());
    }
    
    public static PrimitiveExplainer getInstance(BigDecimal num)
    {
        return PrimitiveExplainer.getInstance(num.doubleValue());
    }

    private final Type type;
    private final Object o;
    
    public PrimitiveExplainer (Type type, Object o)
    {
        if (type.generalType() != Type.GeneralType.SCALAR_VALUE)
            throw new IllegalArgumentException("Type must be a SCALAR_VALUE");
        this.type = type;
        this.o = o;
    }       
    
    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public Object get()
    {
        return o;
    }
}
