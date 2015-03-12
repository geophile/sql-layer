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
package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.SequenceValuesValid;
import com.foundationdb.server.error.SequenceIntervalZeroException;
import com.foundationdb.server.error.SequenceLimitExceededException;
import com.foundationdb.server.error.SequenceMinGEMaxException;
import com.foundationdb.server.error.SequenceStartInRangeException;
import com.foundationdb.util.JUnitUtils;
import com.foundationdb.util.MultipleCauseException;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SequenceTest
{
    private static final boolean CYCLE = true;
    private static final boolean NO_CYCLE = false;

    private Sequence s(long start, long inc, long min, long max, boolean isCycle) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        Sequence s = Sequence.create(ais, "test", "s", start, inc, min, max, isCycle);
        SequenceValuesValid valuesValid = new SequenceValuesValid();
        ais.validate(Arrays.asList(valuesValid)).throwIfNecessary();
        return s;
    }

    private void expectExceeded(Sequence s, long rawValue) {
        try {
            s.realValueForRawNumber(rawValue);
            fail("Expected limit exceeded");
        } catch(SequenceLimitExceededException e) {
            // Expected
        }
    }

    private void c(Sequence s, long rawNumber, long expectedRealValue) {
        assertEquals("realValueForRawNumber at raw="+rawNumber, expectedRealValue, s.realValueForRawNumber(rawNumber));
    }

    @Test(expected=SequenceIntervalZeroException.class)
    public void zeroInc() {
        s(1, 0, 1, 10, CYCLE);
    }

    @Test(expected=SequenceMinGEMaxException.class)
    public void minEqualMax() {
        s(10, 1, 10, 10, CYCLE);
    }

    @Test
    public void minGreaterMax() {
        JUnitUtils.expectMultipleCause(
            new Runnable() {
                @Override
                public void run() {
                    s(1, 1, 15, 10, CYCLE);
                }
            },
            SequenceMinGEMaxException.class,
            SequenceStartInRangeException.class
        );
    }

    @Test(expected=SequenceStartInRangeException.class)
    public void startLessMin() {
        s(1, 1, 5, 10, CYCLE);
    }

    @Test(expected=SequenceStartInRangeException.class)
    public void startGreaterMax() {
        s(15, 1, 5, 10, CYCLE);
    }

    @Test
    public void firstValueIsStart() {
        Sequence s = s(5, 1, 1, 10, CYCLE);
        c(s, 1, 5);
    }

    @Test
    public void startOne_IncOne_MinOne_MaxTen_Cycle() {
        long min = 1;
        long max = 10;
        Sequence s = s(1, 1, 1, 10, CYCLE);
        for(long i = min; i <= max; ++i) {
            c(s, i, i);
        }
        c(s, max + 1, min);
    }

    @Test
    public void startOne_IncOne_MinOne_MaxTen_NoCycle() {
        long min = 1;
        long max = 10;
        Sequence s = s(1, 1, min, max, NO_CYCLE);
        for(long i = min; i <= max; ++i) {
            c(s, i, i);
        }
        expectExceeded(s, max+1);
    }

    @Test
    public void startOne_IncThree_MinOne_MaxTen_Cycle() {
        long inc = 3;
        long min = 1;
        long max = 10;
        Sequence s = s(1, inc, min, max, CYCLE);
        c(s, 1, 1);
        c(s, 2, 4);
        c(s, 3, 7);
        c(s, 4, 10);
        c(s, 5, 1);
        c(s, 6, 4);
        c(s, 7, 7);
        c(s, 8, 10);
        c(s, 9, 1);
        c(s, 10, 4);
    }

    @Test
    public void startOne_IncThree_MinOne_MaxTen_NoCycle() {
        long inc = 3;
        long min = 1;
        long max = 10;
        Sequence s = s(1, inc, min, max, NO_CYCLE);
        c(s, 1, 1);
        c(s, 2, 4);
        c(s, 3, 7);
        c(s, 4, 10);
        expectExceeded(s, 5);
    }

    @Test
    public void startOne_IncThree_MinNegFive_MaxFive_Cycle() {
        long inc = 3;
        long min = -5;
        long max = 5;
        Sequence s = s(1, inc, min, max, CYCLE);
        c(s, 1, 1);
        c(s, 2, 4);
        c(s, 3, -5);
        c(s, 4, -2);
        c(s, 5, 1);
        c(s, 6, 4);
        c(s, 7, -5);
        c(s, 8, -2);
        c(s, 9, 1);
        c(s, 10, 4);
    }

    @Test
    public void startTwo_IncNegThree_MinNegTwo_MaxSeven_Cycle() {
        long inc = -3;
        long min = -2;
        long max = 7;
        Sequence s = s(2, inc, min, max, CYCLE);
        c(s, 1, 2);
        c(s, 2, -1);
        c(s, 3, 7);
        c(s, 4, 4);
        c(s, 5, 1);
        c(s, 6, -2);
        c(s, 7, 7);
        c(s, 8, 4);
        c(s, 9, 1);
        c(s, 10, -2);
    }

    @Test
    public void cycleWithLongMinAndMaxStartOne() {
        long min = Long.MIN_VALUE;
        long max = Long.MAX_VALUE;
        Sequence s = s(1, 1, min, max, CYCLE);
        c(s, 1, 1);
        c(s, max - 1, max - 1);
        c(s, max, max);
        c(s, max + 1, max + 1);
    }

    @Test
    public void cycleWithLongMinAndMaxStartMin() {
        long inc = 1;
        long min = Long.MIN_VALUE;
        long max = Long.MAX_VALUE;
        Sequence s = s(min, inc, min, max, CYCLE);
        c(s, 1, min);
        c(s, 2, min + 1);
        c(s, max - 1, -3);
        c(s, max + 0, -2);
        c(s, max + 1, -1);
        c(s, max + 2, 0);
        c(s, max + max, max - 2);
        c(s, max + max + 1, max - 1);
    }
}
