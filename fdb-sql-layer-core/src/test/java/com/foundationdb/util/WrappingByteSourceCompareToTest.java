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
package com.foundationdb.util;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class WrappingByteSourceCompareToTest {
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        build(pb, bs(), bs(), CompareResult.EQ);
        build(pb, bs(1), bs(), CompareResult.GT);
        build(pb, bs(), bs(1), CompareResult.LT);

        build(pb, bs(5), bs(5), CompareResult.EQ);
        build(pb, bs(5, 1), bs(5), CompareResult.GT);
        build(pb, bs(5), bs(5, 1), CompareResult.LT);

        build(pb, bs(4), bs(5), CompareResult.LT);
        build(pb, bs(4, 1), bs(5), CompareResult.LT);
        build(pb, bs(4), bs(5, 1), CompareResult.LT);

        build(pb, bs(6), bs(5), CompareResult.GT);
        build(pb, bs(6, 1), bs(5), CompareResult.GT);
        build(pb, bs(6), bs(5, 1), CompareResult.GT);

        return pb.asList();
    }

    @Test
    public void normal() {
        test(new WrappingByteSource(bytesOne), new WrappingByteSource(bytesTwo));
    }

    @Test
    public void oneIsOffset() {
        test(wrapWithOffset(bytesOne), new WrappingByteSource(bytesTwo));
    }

    @Test
    public void twoIsOffset() {
        test(new WrappingByteSource(bytesOne), wrapWithOffset(bytesTwo));
    }

    private ByteSource wrapWithOffset(byte[] target) {
        byte[] withOffset = new byte[target.length + OFFSET];
        Arrays.fill(withOffset, (byte) 31);
        System.arraycopy(target, 0, withOffset, OFFSET, target.length);
        return new WrappingByteSource().wrap(withOffset, OFFSET, target.length);
    }

    public WrappingByteSourceCompareToTest(byte[] bytesOne, byte[] bytesTwo, CompareResult expected) {
        this.bytesOne = bytesOne;
        this.bytesTwo = bytesTwo;
        this.expected = expected;
    }

    private final byte[] bytesOne;
    private final byte[] bytesTwo;
    private final CompareResult expected;

    private void test(ByteSource one, ByteSource two) {
        CompareResult actual = CompareResult.fromInt(one.compareTo(two));
        assertEquals(expected, actual);
    }

    private static void build(ParameterizationBuilder pb, byte[] one, byte[] two, CompareResult expected) {
        StringBuilder sb = new StringBuilder();
        sb.append(Arrays.toString(one)).append(' ').append(expected).append(' ').append(Arrays.toString(two));
        pb.add(sb.toString(), one, two, expected);
    }

    private static byte[] bs(int... ints) {
        byte[] bytes = new byte[ints.length];
        for (int i=0; i < ints.length; ++i) {
            bytes[i] = (byte)ints[i];
        }
        return bytes;
    }

    private static final int OFFSET = 4;

    private enum CompareResult {
        LT,
        EQ,
        GT
        ;

        public static CompareResult fromInt(int compareToResult) {
            if (compareToResult < 0)
                return LT;
            return (compareToResult > 0) ? GT : EQ;
        }
    }
}
