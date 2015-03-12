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

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;
import com.google.common.primitives.Longs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.foundationdb.server.types.value.ValueSources.getLong;

@SuppressWarnings("unused") // via reflection
public final class MKeyComparables {

    public static final TKeyComparable[] intComparisons = createIntComparisons();

    private static TKeyComparable[] createIntComparisons() {
        
        final TComparison integerComparison = new TComparison() {
            @Override
            public int compare(TInstance leftInstance, ValueSource left, TInstance rightInstance, ValueSource right) {
                if(left.isNull()) {
                    if(right.isNull()) {
                        return 0;
                    }
                    return -1;
                }
                if(right.isNull()) {
                    return 1;
                }
                return Longs.compare(getLong(left), getLong(right));
            }

            @Override
            public void copyComparables(ValueSource source, ValueTarget target)
            {
                ValueTargets.putLong(target, getLong(source));
            }
        };

        List<TClass> candidates = Arrays.<TClass>asList(
                MNumeric.TINYINT, MNumeric.SMALLINT, MNumeric.MEDIUMINT, MNumeric.INT, MNumeric.BIGINT,
                MNumeric.TINYINT_UNSIGNED, MNumeric.SMALLINT_UNSIGNED, MNumeric.MEDIUMINT_UNSIGNED,
                MNumeric.INT_UNSIGNED
                // BIGINT UNSIGNED is not here, because its representation is not a signed long
        );

        Set<Set<TClass>> alreadyCreated = new HashSet<>();
        List<TKeyComparable> results = new ArrayList<>();
        for (TClass outer : candidates) {
            for (TClass inner : candidates) {
                if (inner == outer)
                    continue;
                Set<TClass> pair = new HashSet<>(Arrays.asList(inner, outer));
                if (alreadyCreated.add(pair)) {
                    results.add(new TKeyComparable(outer, inner, integerComparison));
                }
            }
        }
        return results.toArray(new TKeyComparable[results.size()]);
    }
    
    
    private MKeyComparables() {}
}
