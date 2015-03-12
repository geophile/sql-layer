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
package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.Matchers.IndexMatcher;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.List;

public class MSubstringIndex extends TScalarBase {

    public static final TScalar INSTANCE = new MSubstringIndex();
    
    private static final int MATCHER_INDEX = 0;

    private MSubstringIndex(){}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0).covers(MString.VARCHAR, 1);
        builder.covers(MNumeric.INT, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        String str = inputs.get(0).getString();
        String substr = inputs.get(1).getString();
        int count = inputs.get(2).getInt32();
        boolean signed;

        if (count == 0 || str.isEmpty() || substr.isEmpty()) {
            output.putString("", null);
            return;
        } else if (signed = count < 0) {
            count = -count;
            str = new StringBuilder(str).reverse().toString();
            substr = new StringBuilder(substr).reverse().toString();
        }

        // try to reuse compiled pattern if possible
        IndexMatcher matcher = (IndexMatcher)context.exectimeObjectAt(MATCHER_INDEX);
        if (matcher == null || !matcher.sameState(substr, '\\')) {
            context.putExectimeObject(MATCHER_INDEX, matcher = new IndexMatcher(substr));
        }

        int index = matcher.matchesAt(str, count);
        String ret = index < 0 // no match found
                ? str
                : str.substring(0, index);
        if (signed) {
            ret = new StringBuilder(ret).reverse().toString();
        }

        output.putString(ret, null);
    }

    @Override
    public String displayName() {
        return "SUBSTRING_INDEX";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TInstance stringInstance = inputs.get(0).type();
                return MString.VARCHAR.instance(
                        stringInstance.attribute(StringAttribute.MAX_LENGTH),
                        anyContaminatingNulls(inputs));
            }
        });
    }
}
