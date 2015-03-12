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

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public abstract class MinMaxScalar extends TScalarBase {
    public static final TScalar INSTANCES[] = new TScalar[]
    {
        new MinMaxScalar ("_min") {
            @Override
            protected int getIndex(int comparison) {
                return comparison < 0 ? 0 : 1;
            }
        },
        new MinMaxScalar ("_max") {
            @Override
            protected int getIndex(int comparison) {
                return comparison > 0 ? 0 : 1;
            }
        },
    };

    private final String name;
    
    private MinMaxScalar (String name) {
        this.name = name;
    }

    @Override
    public String displayName() {
        return name;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.pickingCovers(null, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends ValueSource> inputs, ValueTarget output) {
        int comparison = TClass.compare(inputs.get(0).getType(), inputs.get(0), inputs.get(1).getType(), inputs.get(1));
        int index = getIndex (comparison);
        ValueTargets.copyFrom(inputs.get(index), output);
    }
    
    protected abstract int getIndex(int comparison);
}
