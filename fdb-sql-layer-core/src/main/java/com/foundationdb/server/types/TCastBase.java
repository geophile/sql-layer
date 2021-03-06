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
package com.foundationdb.server.types;

import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;

public abstract class TCastBase implements TCast
{
        
    private final TClass sourceClass;
    private final TClass targetClass;
    private final Constantness constness;

    protected TCastBase(TClass sourceClass, TClass targetClass)
    {
        this(sourceClass, targetClass, Constantness.UNKNOWN);
    }

    protected TCastBase(TClass sourceClass, TClass targetClass, Constantness constness)
    {
        this.sourceClass = sourceClass;
        this.targetClass = targetClass;
        this.constness = constness;
    }

    @Override
    public void evaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
        if (source.isNull())
            target.putNull();
        else
            doEvaluate(context, source, target);
    }

    @Override
    public TInstance preferredTarget(TPreptimeValue source) {
        return targetClass().instance(source.isNullable()); // you may want to override this, especially for varchars
    }

    protected abstract void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target);

    @Override
    public Constantness constness()
    {
        return constness;
    }

    @Override
    public TClass sourceClass()
    {
        return sourceClass;
    }

    @Override
    public TClass targetClass()
    {
        return targetClass;
    }

    @Override
    public String toString() {
        return sourceClass + "->" + targetClass;
    }
}
