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
package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.aksql.aktypes.AkInterval;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class Cast_From_AkIntervalSeconds {
    
    public static TCast TO_DOUBLE = new TCastBase(AkInterval.SECONDS, MApproximateNumber.DOUBLE) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            long raw = source.getInt64();
            double result = 0;

            boolean isNegative;
            if (raw < 0) {
                isNegative = true;
                raw = -raw;
            }
            else {
                isNegative = false;
            }

            // we rely here on the fact that the EnumMap sorts entries by ordinal, _and_ that the ordinals
            // in TimeUnit are organized  by small unit -> big.
            for (Map.Entry<TimeUnit, Double> unitAndMultiplier : placeMultiplierMap.entrySet()) {
                TimeUnit unit = unitAndMultiplier.getKey();

                long asLong = AkInterval.secondsIntervalAs(raw, unit);
                assert (asLong >= 0) && (asLong < 100) : asLong;
                raw -= AkInterval.secondsRawFrom(asLong, unit);

                double multiplier = unitAndMultiplier.getValue();
                result += (multiplier * asLong);
            }

            if (isNegative)
                result *= -1d;

            target.putDouble(result);
        }
    };

    private static EnumMap<TimeUnit, Double> placeMultiplierMap = createPlaceMultiplierMap();

    private static EnumMap<TimeUnit, Double> createPlaceMultiplierMap() {
        // Desired result is something like 20120802120133.000000
        // Nicer format: yyyyMMddHHmmSS.uuuuuu 
        EnumMap<TimeUnit, Double> result = new EnumMap<>(TimeUnit.class);
        result.put(TimeUnit.MICROSECONDS, 1.0d/1000000.0d);
        result.put(TimeUnit.SECONDS,      1d);
        result.put(TimeUnit.MINUTES,    100d);
        result.put(TimeUnit.HOURS,    10000d);
        result.put(TimeUnit.DAYS,   1000000d);
        return result;
    }
}
