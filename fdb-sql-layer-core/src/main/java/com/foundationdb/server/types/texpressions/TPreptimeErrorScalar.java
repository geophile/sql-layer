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

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.List;

public abstract class TPreptimeErrorScalar extends TScalarBase {
    protected abstract InvalidOperationException error();

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        for (int pos=0; pos < inputs.length; ++pos) {
            builder.covers(inputs[pos], pos);
        }
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        InvalidOperationException ioe = error();
        assert ioe != null : "no exception provided, but one was expected";
        throw ioe;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Constantness constness(TPreptimeContext context, int inputIndex, LazyList<? extends TPreptimeValue> values) {
        return Constantness.NOT_CONST;
    }

    @Override
    public String displayName() {
        return name;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                throw new UnsupportedOperationException();
            }
        });
    }

    protected TPreptimeErrorScalar(int priority, String name, TClass... inputs) {
        this.priority = priority;
        this.name = name;
        this.inputs = inputs;
    }

    private final int priority;
    private final String name;
    private final TClass[] inputs;
}
