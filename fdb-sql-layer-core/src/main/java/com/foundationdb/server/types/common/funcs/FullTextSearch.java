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

import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class FullTextSearch extends TScalarBase
{
    public static TScalar[] create(TClass booleanType, TClass stringType) {
        return new TScalar[] {
            new FullTextSearch(booleanType, null),
            new FullTextSearch(booleanType, stringType),
        };
    }

    private final TClass booleanType, stringType;

    private FullTextSearch(TClass booleanType, TClass stringType) {
        this.booleanType = booleanType;
        this.stringType = stringType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        if (stringType == null)
            builder.covers(booleanType, 0);
        else
            builder.covers(stringType, 0).covers(stringType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
            throw new UnsupportedSQLException("This query is not supported, its definition " +
                                              "is used solely for optimization purposes.");
    }

    @Override
    public String displayName()
    {
        return "full_text_search";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(booleanType);
    }
}
