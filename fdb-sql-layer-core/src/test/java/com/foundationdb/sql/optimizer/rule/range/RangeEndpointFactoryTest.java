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

import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.sql.optimizer.rule.range.TUtils.constant;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.exclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.inclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.nullExclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.segment;
import static org.junit.Assert.assertEquals;

public final class RangeEndpointFactoryTest {
    @Test
    public void nameLtJoe() {
        check(  Comparison.LT,
                constant("Joe"),
                segment(nullExclusive("Joe"), exclusive("Joe"))
        );
    }

    @Test
    public void nameLeJoe() {
        check(  Comparison.LE,
                constant("Joe"),
                segment(nullExclusive("Joe"), inclusive("Joe"))
        );
    }

    @Test
    public void nameGtJoe() {
        check(  Comparison.GT,
                constant("Joe"),
                segment(exclusive("Joe"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void nameGeJoe() {
        check(  Comparison.GE,
                constant("Joe"),
                segment(inclusive("Joe"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void nameEqJoe() {
        check(  Comparison.EQ,
                constant("Joe"),
                segment(inclusive("Joe"), inclusive("Joe"))
        );
    }

    @Test
    public void nameNeJoe() {
        check(  Comparison.NE,
                constant("Joe"),
                segment(nullExclusive("Joe"), exclusive("Joe")),
                segment(exclusive("Joe"), RangeEndpoint.UPPER_WILD)
        );
    }

    private void check(Comparison comparison, ConstantExpression value, RangeSegment... expected) {
        assertEquals(Arrays.asList(expected), RangeSegment.fromComparison(comparison, value));
    }
}
