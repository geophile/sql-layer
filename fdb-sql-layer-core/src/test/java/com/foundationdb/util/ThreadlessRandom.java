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

public final class ThreadlessRandom {

    private int rand;

    public ThreadlessRandom() {
        this( (int)System.currentTimeMillis() );
    }

    public ThreadlessRandom(int seed) {
        this.rand = seed;
    }

    /**
     * Returns the next random number in the sequence
     * @return a pseudo-random number
     */
    public int nextInt() {
        return ( rand = rand(rand) ); // probably the randiest line in the code
    }

    /**
     * Returns the next random number in the sequence, bounded by the given bounds.
     * @param min the minimum value of the random number, inclusive
     * @param max the maximum value of the random number, isShared
     * @return a number N such that {@code min <= N < max}
     * @throws IllegalArgumentException if {@code min >= max}
     */
    public int nextInt(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException(String.format("bad range: [%d, %d)", min, max));
        }
        int range = max - min;
        int ret = nextInt();
        ret = Math.abs(ret) % range;
        return ret + min;
    }

    /**
     * Quick and dirty pseudo-random generator with no concurrency ramifications.
     * Taken from JCIP; the source is public domain. See:
     * http://jcip.net/listings.html listing 12.4.
     * @param seed the random's seed
     * @return the randomized result
     */
    public static int rand(int seed) {
        seed ^= (seed << 6);
        seed ^= (seed >>> 21);
        seed ^= (seed << 7);
        return seed;
    }
}
