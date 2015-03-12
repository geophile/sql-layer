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

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.types.*;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.util.Strings;
import java.util.List;

public class Unhex extends TScalarBase {

    private static final int VARBINARY_MAX_LENGTH = 65535;
    
    private final TString varchar;
    private final TBinary varbinary;

    public Unhex(TString varchar, TBinary varbinary) {
        this.varchar = varchar;
        this.varbinary = varbinary;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(varchar, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        String st = inputs.get(0).getString();
        
        try {
            output.putBytes(Strings.parseHexWithout0x(st).byteArray());
        }
        catch (InvalidOperationException e) {
            context.warnClient(e);
            output.putNull();
        }
    }

    @Override
    public String displayName() {
        return "UNHEX";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TPreptimeValue preptimeValue = inputs.get(0);
                int stringLength = preptimeValue.type().attribute(StringAttribute.MAX_LENGTH);
                int varbinLength = stringLength / 2;
                if (varbinLength > VARBINARY_MAX_LENGTH)
                    return varbinary.instance(VARBINARY_MAX_LENGTH, preptimeValue.isNullable());
                else
                    return varbinary.instance(varbinLength, preptimeValue.isNullable());
            }        
        });
    }
}
