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
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class DistanceLatLon extends TScalarBase
{
    private final TBigDecimal decimalType;
    private final TClass doubleType;

    public DistanceLatLon(TBigDecimal decimalType, TClass doubleType) {
        this.decimalType = decimalType;
        this.doubleType = doubleType;
    }
    
    public static final double MAX_LAT = 90;
    public static final double MIN_LAT = -90;
    public static final double MAX_LON = 180;
    public static final double MIN_LON = -180;
    
    public static final double MAX_LON_DIS = MAX_LON * 2;
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(decimalType, 0, 1, 2, 3);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        ValueSource input0 = inputs.get(0);
        ValueSource input1 = inputs.get(1);
        ValueSource input2 = inputs.get(2);
        ValueSource input3 = inputs.get(3);

        double y1 = doubleInRange(TBigDecimal.getWrapper(input0, input0.getType()), MIN_LAT, MAX_LAT);
        double x1 = doubleInRange(TBigDecimal.getWrapper(input1, input1.getType()), MIN_LON, MAX_LON);
        double y2 = doubleInRange(TBigDecimal.getWrapper(input2, input2.getType()), MIN_LAT, MAX_LAT);
        double x2 = doubleInRange(TBigDecimal.getWrapper(input3, input3.getType()), MIN_LON, MAX_LON);
        
        double dx = Math.abs(x1 - x2);
        // we want the shorter distance of the two
        if (Double.compare(dx, MAX_LON) > 0)
            dx = MAX_LON_DIS - dx;
        
        double dy = y1 - y2;
        
        output.putDouble(Math.sqrt(dx * dx + dy * dy));
    }

    public static double doubleInRange(BigDecimalWrapper val, double min, double max)
    {
        double dVar = val.asBigDecimal().doubleValue();
        
        if (Double.compare(dVar, min) >= 0 && Double.compare(dVar, max) <= 0)
            return dVar;
        else
            throw new InvalidParameterValueException(String.format("Value out of range[%f, %f]: %f ", min, max, dVar));
    }
    @Override
    public String displayName()
    {
        return "DISTANCE_LAT_LON";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(doubleType);
    }
}
