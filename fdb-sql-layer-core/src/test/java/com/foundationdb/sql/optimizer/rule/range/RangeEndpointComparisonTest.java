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
package com.foundationdb.sql.optimizer.rule.range;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.foundationdb.sql.optimizer.rule.range.ComparisonResult.*;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.exclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.inclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.nullExclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.nullInclusive;
import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class RangeEndpointComparisonTest {
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // nulls vs nulls
        param(pb,  nullInclusive(AARDVARK), EQ,  nullInclusive(AARDVARK));
        param(pb,  nullInclusive(AARDVARK), LT_BARELY,  nullExclusive(AARDVARK));
        param(pb,  nullExclusive(AARDVARK), EQ,  nullExclusive(AARDVARK));

        // nulls vs "normal" values
        param(pb,  nullInclusive(AARDVARK), LT, inclusive(AARDVARK));
        param(pb,  nullInclusive(AARDVARK), LT, exclusive(AARDVARK));
        param(pb,  nullExclusive(AARDVARK), LT, inclusive(AARDVARK));
        param(pb,  nullExclusive(AARDVARK), LT, exclusive(AARDVARK));

        // nulls vs wild
        param(pb,  nullInclusive(AARDVARK), LT, RangeEndpoint.UPPER_WILD);
        param(pb,  nullExclusive(AARDVARK), LT, RangeEndpoint.UPPER_WILD);

        // normal values vs same values
        param(pb, inclusive(AARDVARK), EQ, inclusive(AARDVARK));
        param(pb, inclusive(AARDVARK), LT_BARELY, exclusive(AARDVARK));

        // normal values vs comparable values
        param(pb, inclusive(AARDVARK), LT, inclusive(CAT));
        param(pb, inclusive(AARDVARK), LT, exclusive(CAT));
        param(pb, exclusive(AARDVARK), LT, inclusive(CAT));
        param(pb, exclusive(AARDVARK), LT, exclusive(CAT));

        // normal values vs wild
        param(pb, inclusive(AARDVARK), LT, RangeEndpoint.UPPER_WILD);
        param(pb, exclusive(AARDVARK), LT, RangeEndpoint.UPPER_WILD);

        // wild vs wild
        param(pb, RangeEndpoint.UPPER_WILD, EQ, RangeEndpoint.UPPER_WILD);

        // incomparable types
        param(pb, inclusive(TWO), INVALID, inclusive(AARDVARK));
        param(pb, inclusive(TWO), INVALID, exclusive(AARDVARK));
        param(pb, exclusive(TWO), INVALID, inclusive(AARDVARK));
        param(pb, exclusive(TWO), INVALID, exclusive(AARDVARK));

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb,
                              RangeEndpoint one, ComparisonResult expected, RangeEndpoint two)
    {
        String name = one + " " + expected.describe() + " " + two;
        pb.add(name, one, two, expected);
        // test reflectivity
        final ComparisonResult flippedExpected;
        switch (expected) {
        case LT:        flippedExpected = GT;           break;
        case LT_BARELY: flippedExpected = GT_BARELY;    break;
        case GT:        flippedExpected = LT;           break;
        case GT_BARELY: flippedExpected = LT_BARELY;    break;
        default:        flippedExpected = expected;     break;
        }
        String flippedName = two + " " + flippedExpected.describe() + " " + one;
        if (!flippedName.equals(name)) { // e.g. we don't need to reflect inclusive("A") == inclusive("A")
            pb.add(flippedName, two, one, flippedExpected);
        }
    }

    private static String AARDVARK = "aardvark";
    private static String CAT = "cat";
    private static long TWO = 2;

    @Test
    public void compare() {
        assertEquals(expected, one.comparePreciselyTo(two));
    }

    public RangeEndpointComparisonTest(RangeEndpoint one, RangeEndpoint two, ComparisonResult expected) {
        this.one = one;
        this.two = two;
        this.expected = expected;
    }

    private final RangeEndpoint one;
    private final RangeEndpoint two;
    private final ComparisonResult expected;
}
