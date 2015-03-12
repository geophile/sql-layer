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

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public abstract class MPeriodArith extends TScalarBase {

    private final String name;
    
    public static final TScalar[] INSTANCES = {
        new MPeriodArith("PERIOD_ADD") {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
                long period = inputs.get(0).getInt64();
                long offsetMonths = inputs.get(1).getInt64();

                // COMPATIBILITY: MySQL currently has undefined behavior for negative numbers
                // Our behavior follows our B.C. year numbering (-199402 + 1 = -199401)
                // Java's mod returns negative numbers: -1994 % 100 = -94
                long periodInMonths = fromPeriod(period);
                long totalMonths = periodInMonths + offsetMonths;

                // Handle the case where the period changes sign (e.g. -YYMM to YYMM)
                // as a result of adding. Since 0000 months is not really a date,
                // this leads to an off by one error
                if (Long.signum(periodInMonths) * Long.signum(totalMonths) == -1) {
                    totalMonths -= Long.signum(totalMonths) * 2;
                }

                long result = toPeriod(totalMonths);
                output.putInt64(result);
            }
        },
        new MPeriodArith("PERIOD_DIFF") {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
                // COMPATIBILITY: MySQL currently has undefined behavior for negative numbers
                // Our behavior follows our B.C. year numbering (-199402 + 1 = -199401)
                long periodLeft = inputs.get(0).getInt64();
                long periodRight = inputs.get(1).getInt64();

                long result = fromPeriod(periodLeft) - fromPeriod(periodRight);
                output.putInt64(result);
            }
        }
    };
            
    private MPeriodArith(String name) {
        this.name = name;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MNumeric.BIGINT, 0, 1);
    }

    @Override
    public String displayName() {
        return name;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.BIGINT);
    }

    // Helper functions
    // Takes a period and returns the number of months from year 0
    protected static long fromPeriod(long period)
    {
        int periodSign = Long.signum(period);
        
        long rawMonth = period % 100;
        long rawYear = period / 100;

        long absValYear = Math.abs(rawYear);
        if (absValYear < 70)
            rawYear += periodSign * 2000;
        else if (absValYear < 100)
            rawYear += periodSign * 1900; 
        
        return (rawYear * 12 + rawMonth - (1 * periodSign));
    }
    
    // Create a YYYYMM format from a number of months
    protected static long toPeriod(long monthCount)
    {
        long year = monthCount / 12;
        long month = (monthCount % 12) + 1 * Long.signum(monthCount);
        return (year * 100) + month;
    }
    
    // End helper functions ***************************************************
}
