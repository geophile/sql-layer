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
package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public abstract class TArithmetic extends TScalarBase {

    protected TArithmetic(String overloadName, TClass operand0, TClass operand1, TClass resultType, int... attrs) {
       this.overloadName = overloadName;
       this.operand0 = operand0;
       this.operand1 = operand1;
       this.resultType = resultType;
       this.attrs = attrs;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        TInstanceNormalizer normalizer = inputSetInstanceNormalizer();
        if (operand0 == operand1)
            builder.nextInputPicksWith(normalizer).covers(operand0, 0, 1);
        else {
            builder.nextInputPicksWith(normalizer).covers(operand0, 0);
            builder.nextInputPicksWith(normalizer).covers(operand1, 1);
        }
    }

    @Override
    public String displayName() {
        return overloadName;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(resultType, attrs);
    }

    protected TInstanceNormalizer inputSetInstanceNormalizer() {
        return null;
    }

    private final String overloadName;
    private final TClass operand0;
    private final TClass operand1;
    private final TClass resultType;
    private final int[] attrs;
}
