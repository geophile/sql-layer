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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.regex.Pattern;

public class TRegex extends TScalarBase
{
    private static final int CACHE_INDEX = 0;

    public static TScalar[] create(TClass stringType) {
        int[] withoutOpts = { 0, 1 }, withOpts = { 0, 1, 2 };
        return new TRegex[] {
            new TRegex(stringType, withoutOpts, true),
            new TRegex(stringType, withoutOpts, false),
            new TRegex(stringType, withOpts, true),
            new TRegex(stringType, withOpts, false),
        };
    }

    private final int[] covering;
    private final TClass stringType;
    private final boolean caseSensitive;

    private TRegex(TClass stringType, int[] covering, boolean caseSensitive) {
        this.covering = covering;
        this.stringType = stringType;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public String displayName() {
        return caseSensitive ? "REGEX" : "IREGEX";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBool.INSTANCE);
    }

    @Override
    public TPreptimeValue evaluateConstant(TPreptimeContext context, LazyList<? extends TPreptimeValue> inputs) {
        TPreptimeValue result = super.evaluateConstant(context, inputs);
        if (result != null) {
            // Already constant
            return result;
        }
        ValueSource pattern = inputs.get(1).value();
        if(pattern == null) {
            return null; // Dynamic pattern
        }
        int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;
        if(covering.length == 3) {
            ValueSource opts = inputs.get(2).value();
            if(opts == null) {
                return null; // Dynamic opts
            }
            flags |= parseOptionFlags(opts.getString());
        }
        Pattern p = Pattern.compile(pattern.getString(), flags);
        context.set(CACHE_INDEX, p);
        ValueSource inputValue = inputs.get(0).value();
        if(inputValue != null) {
            // Constant input
            boolean matches = p.matcher(inputValue.getString()).find();
            Value value = new Value(context.getOutputType(), matches);
            return new TPreptimeValue(value);
        }

        return null;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        if (covering.length == 2) {
            builder.covers(stringType, 0).covers(stringType, 1);
        } else if (covering.length == 3) {
            builder.covers(stringType, 0).covers(stringType,1).covers(stringType, 2);
        } else {
            assert false : "TRegex input set covering is not length 2 or 3";
        }
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        Pattern p = (Pattern)context.preptimeObjectAt(CACHE_INDEX);
        if(p == null) {
            String pattern = inputs.get(1).getString();
            int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;
            if(covering.length == 3) {
                String opts = inputs.get(2).getString();
                flags |= parseOptionFlags(opts);
            }
            p = (Pattern)context.exectimeObjectAt(CACHE_INDEX);
            if((p == null) || (p.pattern() != pattern) || (p.flags() != flags)) {
                p = Pattern.compile(pattern, flags);
                context.putExectimeObject(CACHE_INDEX, p);
            }
        }
        String input = inputs.get(0).getString();
        output.putBool(p.matcher(input).find());
    }

    private static int parseOptionFlags(String opts) {
        int flags = 0;
        for(int i = 0; i < opts.length(); ++i) {
            switch(opts.charAt(i)) {
                // Standard 'special construct match flags'
                case 'i': flags |= Pattern.CASE_INSENSITIVE; break;
                case 'd': flags |= Pattern.UNIX_LINES; break;
                case 'm': flags |= Pattern.MULTILINE; break;
                case 's': flags |= Pattern.DOTALL; break;
                case 'u': flags |= Pattern.UNICODE_CASE; break;
                case 'x': flags |= Pattern.COMMENTS; break;
                // And pick a letters for remaining flags
                case 'l': flags |= Pattern.LITERAL; break;
                case 'c': flags |= Pattern.CANON_EQ; break;
                default:
                    throw new InvalidParameterValueException("Invalid option: " + opts.charAt(i));
            }
        }
        return flags;
    }
}
