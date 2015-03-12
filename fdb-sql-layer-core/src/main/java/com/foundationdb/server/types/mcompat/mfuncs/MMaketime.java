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
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class MMaketime extends TScalarBase {

    public static final TScalar INSTANCE = new MMaketime() {};
    
    private MMaketime() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.INT, 0, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        // Time input format HHMMSS
        int hours = inputs.get(0).getInt32();
        int minutes = inputs.get(1).getInt32();
        int seconds = inputs.get(2).getInt32();
        
        // Check for invalid input
        if (minutes < 0 || minutes >= 60 || seconds < 0 || seconds >= 60) {
            output.putNull();
            return;
        }
        
        int mul;
        if (hours < 0)
            hours *= mul = -1;
        else
            mul = 1;

        output.putInt32(MDateAndTime.encodeTime(hours, minutes, seconds, context));
    }

    @Override
    public String displayName() {
        return "MAKETIME";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MDateAndTime.TIME);
    }
}
