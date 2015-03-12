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
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ConditionExpression;
import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.foundationdb.sql.optimizer.rule.range.TUtils.*;
import static org.junit.Assert.assertEquals;

public final class ColumnRangesTest {

    @Test
    public void colLtValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.LT, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(nullExclusive("joe"), exclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueLtCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.LT, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(exclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colLeValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.LE, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(nullExclusive("joe"), inclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueLeCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.LE, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(inclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colGtValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.GT, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(exclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueGtCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.GT, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(nullExclusive("joe"), exclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colGeValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.GE, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(inclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueGeCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.GE, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(nullExclusive("joe"), inclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colEqValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.EQ, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(inclusive("joe"), inclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueEqCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.EQ, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(inclusive("joe"), inclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void colNeValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(firstName, Comparison.NE, value);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                    segment(nullExclusive("joe"), exclusive("joe")),
                    segment(exclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void valueNeCol() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = compare(value, Comparison.NE, firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                compare,
                segment(nullExclusive("joe"), exclusive("joe")),
                segment(exclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }
    
    @Test
    public void notColLtValue() {
        ConstantExpression value = constant("joe");
        ConditionExpression compare = not(compare(value, Comparison.LT, firstName));
        ColumnRanges expected = null;
        assertEquals(expected, ColumnRanges.rangeAtNode(compare));
    }

    @Test
    public void columnIsNull() {
        ConditionExpression isNull = isNull(firstName);
        ColumnRanges expected = columnRanges(
                firstName,
                isNull,
                segment(RangeEndpoint.nullInclusive(firstName), RangeEndpoint.nullInclusive(firstName))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(isNull));
    }

    @Test
    public void differentColumns() {
        ConditionExpression firstNameLtJoe = compare(firstName, Comparison.LT, constant("joe"));
        ConditionExpression lastNameLtSmith = compare(lastName, Comparison.LT, constant("smith"));
        ConditionExpression either = or(firstNameLtJoe, lastNameLtSmith);
        ColumnRanges expected = null;
        assertEquals(expected, ColumnRanges.rangeAtNode(either));
    }

    // the and/or tests are pretty sparse, since RangeSegmentTest is more exhaustive about
    // the overlaps and permutations.

    @Test
    public void orNoOverlap() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("abe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("joe"));
        ConditionExpression either = or(nameLtAbe, nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                either,
                segment(nullExclusive("joe"), exclusive("abe")),
                segment(inclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(either));
    }

    @Test
    public void orWithOverlap() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("joe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("abe"));
        ConditionExpression either = or(nameLtAbe, nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                either,
                segment(nullExclusive("joe"), RangeEndpoint.UPPER_WILD)
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(either));
    }

    @Test
    public void andNoOverlap() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("abe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("joe"));
        ConditionExpression both = and(nameLtAbe, nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                both
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(both));
    }

    @Test
    public void andWithOverlap() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("joe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("abe"));
        ConditionExpression both = and(nameLtAbe, nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                both,
                segment(inclusive("abe"), exclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.rangeAtNode(both));
    }

    @Test
    public void explicitAnd() {
        ConditionExpression nameLtAbe = compare(firstName, Comparison.LT, constant("joe"));
        ConditionExpression nameGeJoe = compare(firstName, Comparison.GE, constant("abe"));
        ColumnRanges nameLtAbeRanges = ColumnRanges.rangeAtNode(nameLtAbe);
        ColumnRanges nameGeJoeRanges = ColumnRanges.rangeAtNode(nameGeJoe);
        ColumnRanges expected = columnRanges(
                firstName,
                set(nameLtAbe, nameGeJoe),
                segment(inclusive("abe"), exclusive("joe"))
        );
        assertEquals(expected, ColumnRanges.andRanges(nameLtAbeRanges, nameGeJoeRanges));
        assertEquals(expected, ColumnRanges.andRanges(nameGeJoeRanges, nameLtAbeRanges));
    }

    @Test
    public void sinOfColumn() {
        ConditionExpression isNull = sin(firstName);
        ColumnRanges expected = null;
        assertEquals(expected, ColumnRanges.rangeAtNode(isNull));
    }
    
    private ColumnRanges columnRanges(ColumnExpression col, ConditionExpression condition, RangeSegment... segments) {
        return new ColumnRanges(
                col,
                Collections.singleton(condition),
                Arrays.asList(segments)
        );
    }

    private ColumnRanges columnRanges(ColumnExpression col, Set<ConditionExpression> conditions, RangeSegment... segs) {
        return new ColumnRanges(
                col,
                conditions,
                Arrays.asList(segs)
        );
    }

    private Set<ConditionExpression> set(ConditionExpression... args) {
        Set<ConditionExpression> result = new HashSet<>();
        for (ConditionExpression object : args) {
            result.add(object);
        }
        return result;
    }
}
