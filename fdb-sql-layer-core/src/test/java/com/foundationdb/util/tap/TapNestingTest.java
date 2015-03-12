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
package com.foundationdb.util.tap;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class TapNestingTest
{
    @Before
    public void before()
    {
        Tap.DISPATCHES.clear();
        Tap.registerBadNestingHandler(
            new Tap.BadNestingHandler()
            {
                @Override
                public void handleBadNesting(Tap tap)
                {
                    throw new BadNestingException();
                }
            });
    }

    @Test
    public void testOK()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        tap.out();
    }

    @Test(expected = BadNestingException.class)
    public void testReenter()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        tap.in();
    }

    @Test(expected = BadNestingException.class)
    public void testUnexpectedExit()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        tap.out();
        tap.out();
    }

    @Test(expected = BadNestingException.class)
    public void testUnexpectedExitSomeMore()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        tap.out();
        tap.out();
    }
    
    @Test
    public void testInEnableOut()
    {
        InOutTap tap = Tap.createTimer("test");
        disable();
        tap.in();
        enable();
        tap.out();
    }

    @Test
    public void testEnableOutInOut()
    {
        InOutTap tap = Tap.createTimer("test");
        disable();
        enable();
        tap.out();
        tap.in();
        tap.out();
    }

    @Test
    public void testTemporaryDisable()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        disable();
        tap.out();
        enable();
        tap.in();
    }

    @Test
    public void testRandomTemporaryDisable()
    {
        final int N = 100000;
        Random random = new Random();
        random.setSeed(419);
        InOutTap tap = Tap.createTimer("test");
        enable();
        boolean enabled = true;
        for (int i = 0; i < N; i++) {
            if ((i % 2) == 0) {
                tap.in();
            } else {
                tap.out();
            }
            if ((random.nextInt() % 3) == 0) {
                if (enabled) {
                    disable();
                    enabled = false;
                } else {
                    enable();
                    enabled = true;
                }
            }
        }
    }

    private void disable()
    {
        Tap.setEnabled(ALL_TAPS, false);
    }

    private void enable()
    {
        Tap.setEnabled(ALL_TAPS, true);
    }

    private static final String ALL_TAPS = ".*";
    private static class BadNestingException extends RuntimeException
    {
        public BadNestingException()
        {
        }
    }
}
