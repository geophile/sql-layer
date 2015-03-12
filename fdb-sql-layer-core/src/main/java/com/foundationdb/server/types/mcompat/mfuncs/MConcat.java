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

import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

import java.util.List;

public class MConcat extends TScalarBase {
    public static final TScalar INSTANCE = new MConcat();
    
    private MConcat(){}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.nextInputPicksWith(MString.VARCHAR.PICK_RIGHT_LENGTH).vararg(MString.VARCHAR, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < inputs.size(); ++i) {
            String inputStr = inputs.get(i).getString();
            assert inputStr != null;
            sb.append(inputStr);
        }
        output.putString(sb.toString(), null);
    }

    @Override
    public String displayName() {
        return "concatenate";
    }

    @Override
    public String[] registeredNames() {
        return new String[] { "concatenate", "concat" };
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                int length = 0;
                for (TPreptimeValue ptv : inputs) {
                    length += ptv.type().attribute(StringAttribute.MAX_LENGTH);
                }
                return MString.VARCHAR.instance(length, anyContaminatingNulls(inputs));
            }
        });
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context, List<? extends TPreparedExpression> inputs, TInstance resultType)
    {
        CompoundExplainer ex = super.getExplainer(context, inputs, resultType);
        ex.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance("||"));
        ex.addAttribute(Label.ASSOCIATIVE, PrimitiveExplainer.getInstance(true));
        return ex;
    }
}
