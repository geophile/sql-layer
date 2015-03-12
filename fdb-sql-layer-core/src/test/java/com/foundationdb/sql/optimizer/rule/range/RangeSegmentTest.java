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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.foundationdb.sql.optimizer.rule.range.TUtils.exclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.inclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.nullExclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.nullInclusive;
import static com.foundationdb.sql.optimizer.rule.range.TUtils.segment;
import static com.foundationdb.util.AssertUtils.assertCollectionEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public final class RangeSegmentTest {

    @Test
    public void sacEmptyList() {
        List<RangeSegment> original = Collections.emptyList();
        sacAndCheck(original, Collections.<RangeSegment>emptyList());
    }

    @Test
    public void sacUnchanged() {
        List<RangeSegment> original = Arrays.asList(
                segment(nullExclusive("apple"), exclusive("apple")),
                segment(exclusive("apple"), exclusive("orange")),
                segment(exclusive("orange"), RangeEndpoint.UPPER_WILD)
        );
        List<RangeSegment> copy = new ArrayList<>(original);
        sacAndCheck(original, copy);
    }

    @Test
    public void sacSortNoCombine() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("orange")),
                segment(exclusive("orange"), RangeEndpoint.UPPER_WILD),
                segment(nullExclusive("apple"), exclusive("apple"))
        );
        sacAndCheck(
                original,
                segment(nullExclusive("apple"), exclusive("apple")),
                segment(exclusive("apple"), exclusive("orange")),
                segment(exclusive("orange"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void sacCombineInclusiveInclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), inclusive("apple")),
                segment(inclusive("apple"), inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), inclusive("orange"))
        );
    }

    @Test
    public void sacCombineInclusiveExclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), inclusive("apple")),
                segment(exclusive("apple"), inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), inclusive("orange"))
        );
    }

    @Test
    public void sacCombineExclusiveExclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), exclusive("apple")),
                segment(exclusive("apple"), inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), exclusive("apple")),
                segment(exclusive("apple"), inclusive("orange"))
        );
    }

    @Test
    public void sacCombineExclusiveInclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), exclusive("apple")),
                segment(inclusive("apple"), inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), inclusive("orange"))
        );
    }

    @Test
    public void sacCombineStartExclusiveInclusiveNulls() {
        List<RangeSegment> original = Arrays.asList(
                segment(nullExclusive("apple"), exclusive("apple")),
                segment(nullInclusive("banana"), inclusive("banana"))
        );
        sacAndCheck(
                original,
                segment(nullInclusive("banana"), inclusive("banana"))
        );
    }

    @Test
    public void sacCombineStartInclusiveExclusiveNulls() {
        List<RangeSegment> original = Arrays.asList(
                segment(nullInclusive("apple"), exclusive("apple")),
                segment(nullExclusive("banana"), inclusive("banana"))
        );
        sacAndCheck(
                original,
                segment(nullInclusive("banana"), inclusive("banana"))
        );
    }

    @Test
    public void sacCombineStartExclusiveInclusiveStrings() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("banana")),
                segment(inclusive("apple"), inclusive("mango"))
        );
        sacAndCheck(
                original,
                segment(inclusive("apple"), inclusive("mango"))
        );
    }

    @Test
    public void sacCombineStartInclusiveExclusiveStrings() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("apple"), exclusive("banana")),
                segment(exclusive("apple"), inclusive("mango"))
        );
        sacAndCheck(
                original,
                segment(inclusive("apple"), inclusive("mango"))
        );
    }

    @Test
    public void sacCombineEndExclusiveInclusiveStrings() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("mango")),
                segment(inclusive("banana"), inclusive("mango"))
        );
        sacAndCheck(
                original,
                segment(exclusive("apple"), inclusive("mango"))
        );
    }

    @Test
    public void sacCombineEndInclusiveExclusiveStrings() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), inclusive("mango")),
                segment(inclusive("banana"), exclusive("mango"))
        );
        sacAndCheck(
                original,
                segment(exclusive("apple"), inclusive("mango"))
        );
    }

    @Test
    public void sacSubset() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("zebra")),
                segment(inclusive("cactus"), inclusive("person"))
        );
        sacAndCheck(
                original,
                segment(exclusive("apple"), exclusive("zebra"))
        );
    }

    @Test
    public void sacOverlapNoWilds() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("person")),
                segment(inclusive("cactus"), inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(exclusive("apple"), inclusive("zebra"))
        );
    }

    @Test
    public void sacOverlapWildStart() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("person")),
                segment(nullExclusive("zebra"), inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(nullExclusive("zebra"), inclusive("zebra"))
        );
    }

    @Test
    public void sacOverlapWildEnd() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), RangeEndpoint.UPPER_WILD),
                segment(exclusive("apple"), exclusive("person"))
        );
        sacAndCheck(
                original,
                segment(inclusive("aardvark"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void sacKeepStartAndEndInclusivity() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), inclusive("person")),
                segment(inclusive("cat"), inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(inclusive("aardvark"), inclusive("zebra"))
        );
    }
    @Test
    public void sacKeepStartExclusivity() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), inclusive("person")),
                segment(inclusive("cat"), inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), inclusive("zebra"))
        );
    }
    
    @Test
    public void sacKeepEndExclusivity() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), inclusive("person")),
                segment(inclusive("cat"), exclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(inclusive("aardvark"), exclusive("zebra"))
        );
    }

    @Test
    public void sacIncomparableTypesForSorting() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), inclusive("person")),
                segment(inclusive(1L), exclusive("zebra"))
        );
        List<RangeSegment> copy = new ArrayList<>(original);
        List<RangeSegment> sorted = RangeSegment.sortAndCombine(copy);
        assertCollectionEquals(original, copy);
        assertNull("sorted list should be null", sorted);
    }

    @Test
    public void sacIncomparableTypesForMerging() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), inclusive(1L)),
                segment(inclusive("cat"), exclusive(1L))
        );
        List<RangeSegment> copy = new ArrayList<>(original);
        List<RangeSegment> sorted = RangeSegment.sortAndCombine(copy);
        assertCollectionEquals(original, copy);
        assertNull("sorted list should be null", sorted);
    }

    @Test(expected = IllegalArgumentException.class)
    public void startIsNull() {
        segment(null, inclusive(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void endIsNull() {
        segment(inclusive(1), null);
    }

    /**
     * checks sortAndCombine (aka sac) by sac'ing (a copy of) the given list, asserting that what comes out
     * is the same instance, and checking it for equality against a list created from the given expected RangeSegments.
     * @param list the list to sac-and-check
     * @param expectedList the expected RangeSegments
     */
    private static void sacAndCheck(List<? extends RangeSegment> list, List<? extends RangeSegment> expectedList) {
        List<RangeSegment> copy = new ArrayList<>(list);
        List<RangeSegment> sorted = RangeSegment.sortAndCombine(copy);
        assertSame(copy, sorted);
        assertCollectionEquals(expectedList, new ArrayList<>(sorted));
    }

    private static void sacAndCheck(List<RangeSegment> list, RangeSegment first, RangeSegment... expecteds) {
        List<RangeSegment> expectedsList = new ArrayList<>();
        expectedsList.add(first);
        Collections.addAll(expectedsList, expecteds);
        sacAndCheck(list, expectedsList);
    }
}
