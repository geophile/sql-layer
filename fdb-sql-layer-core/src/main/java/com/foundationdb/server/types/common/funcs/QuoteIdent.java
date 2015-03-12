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

import java.util.List;

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.Strings;


public class QuoteIdent extends TScalarBase
{
    public static TScalar[] create(TClass varchar, TClass tbool) {
        return new TScalar[] {
                new QuoteIdent(varchar, tbool, varchar, 0),       // ('string')
                new QuoteIdent(varchar, tbool, varchar, 0, 1),    // ('string', 'quote')
                new QuoteIdent(varchar, tbool, varchar, 0, 1, 2), // ('string', 'quote', 'force')
        };
    }

    protected final TClass inputTypeString;
    protected final TClass inputTypeBool;
    protected final TClass outputType;
    protected final int[] covering;

    private QuoteIdent(TClass inputTypeString, TClass inputTypeBool, TClass outputType, int... covering) {
        this.inputTypeString = inputTypeString;
        this.inputTypeBool = inputTypeBool;
        this.outputType = outputType;
        this.covering = covering;
    }

    @Override
    public String displayName() {
        return "QUOTE_IDENT";
    }

    @Override
    public TOverloadResult resultType() {
        // actual return type is exactly the same as input type
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TInstance source = inputs.get(0).type();
                return inputTypeString.instance(source.attribute(StringAttribute.MAX_LENGTH) + 2, 
                        source.attribute(StringAttribute.CHARSET), 
                        source.attribute(StringAttribute.COLLATION),
                        anyContaminatingNulls(inputs));
            }
        });
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        if (covering.length == 1) {
            builder.covers(inputTypeString, covering);
        }
        else if (covering.length == 2) {
            builder.covers(inputTypeString, 0).covers(inputTypeString, 1);
        }
        else {
            builder.covers(inputTypeString, 0).covers(inputTypeString, 1);
            builder.covers(inputTypeBool, 2);
        }
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return true;
    }

    @Override
    protected boolean neverConstant() {
        return false;
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
                              LazyList<? extends ValueSource> inputs,
                              ValueTarget output) {

        // defaults:
        String s    = inputs.get(0).getString();
        char c      = '\"';
        boolean b   = false;

        if( s.isEmpty() )
        {
            throw new InvalidParameterValueException("Empty identifier");
        }

        if( covering.length >= 2) {
            String arg2 = inputs.get(1).getString();

            if (arg2.length() == 1) {
                c = arg2.charAt(0);
            }
            else {
                throw new InvalidParameterValueException(("one character as quote allowed"));
            }
        }

        if ( covering.length == 3) {
            b = inputs.get(2).getBoolean();
        }

        output.putString( Strings.quotedIdent(s, c, b), null );
    }
}
