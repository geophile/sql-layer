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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TapTest {
    private static final int THREADS = 20;
    private static final int CYCLES = 10000;
    
    @Before
    public void before() {
        Tap.DISPATCHES.clear();
    }

    @Test
    public void testPerThreadTap() throws Exception {
        final InOutTap tap = Tap.createTimer("tap");
        Tap.setEnabled(".*", true);
        final Thread[] threads = new Thread[THREADS];
        for (int i = 0; i < THREADS; i++) {
            final Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    for (int j = 0; j < CYCLES; j++) {
                        tap.in();
                        tap.out();
                    }
                }

            }, "thread_" + i);
            threads[i] = thread;
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        TapReport[] reports = tap.getReports();
        assertEquals(1, reports.length);
        TapReport report = reports[0];
        assertEquals(THREADS * CYCLES, report.getInCount());
        assertEquals(THREADS * CYCLES, report.getOutCount());
    }

    // Inspired by bug 869554
    @Test
    public void testConcurrentTapControl() throws InterruptedException
    {
        Thread[] threads = new Thread[THREADS];
        final boolean[] ok = new boolean[THREADS];
        for (int t = 0; t < THREADS; t++) {
            final int threadId = t;
            threads[t] = new Thread()
            {
                @Override
                public void run()
                {
                    try {
                        Tap.createTimer(String.format("tap %s", threadId));
                        for (int i = 0; i < CYCLES; i++) {
                            Tap.setEnabled(".*", (i % 2) == 0);
                        }
                        ok[threadId] = true;
                    } catch (Exception e) {
                        ok[threadId] = false;
                    }
                }
            };
        }
        for (Thread thread : threads) {
            thread.start();
            Thread.sleep(1);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        for (boolean b : ok) {
            assertTrue(b);
        }
    }

    @Test
    public void testInitiallyEnabled()
    {
        Tap.setInitiallyEnabled("^c|d$");
        InOutTap a = Tap.createTimer("a");
        PointTap b = Tap.createCount("b");
        InOutTap c = Tap.createTimer("c");
        PointTap d = Tap.createCount("d");
        TapReport[] reports = Tap.getReport(".*");
        assertEquals(2, reports.length);
        int mask = 0;
        for (TapReport report : reports) {
            if (report.getName().equals("c")) {
                mask |= 0x1;
            } else if (report.getName().equals("d")) {
                mask |= 0x2;
            } else {
                fail();
            }
        }
        assertEquals(0x3, mask);
    }

    @Test
    public void testDisableAll()
    {
        Tap.setInitiallyEnabled("^c|d$");
        InOutTap a = Tap.createTimer("a");
        PointTap b = Tap.createCount("b");
        InOutTap c = Tap.createTimer("c");
        PointTap d = Tap.createCount("d");
        Tap.setEnabled(".*", false);
        TapReport[] reports = Tap.getReport(".*");
        assertEquals(0, reports.length);
    }

    @Test
    public void testEnableAll()
    {
        Tap.setInitiallyEnabled("^c|d$");
        InOutTap a = Tap.createTimer("a");
        PointTap b = Tap.createCount("b");
        InOutTap c = Tap.createTimer("c");
        PointTap d = Tap.createCount("d");
        Tap.setEnabled(".*", false);
        TapReport[] reports = Tap.getReport(".*");
        assertEquals(0, reports.length);
        Tap.setEnabled(".*", true);
        reports = Tap.getReport(".*");
        assertEquals(4, reports.length);
        int mask = 0;
        for (TapReport report : reports) {
            if (report.getName().equals("a")) {
                mask |= 0x1;
            } else if (report.getName().equals("b")) {
                mask |= 0x2;
            } else if (report.getName().equals("c")) {
                mask |= 0x4;
            } else if (report.getName().equals("d")) {
                mask |= 0x8;
            } else {
                fail();
            }
        }
        assertEquals(0xf, mask);
    }

    @Test
    public void testEnableInitial()
    {
        Tap.setInitiallyEnabled("^c|d$");
        InOutTap a = Tap.createTimer("a");
        PointTap b = Tap.createCount("b");
        InOutTap c = Tap.createTimer("c");
        PointTap d = Tap.createCount("d");
        Tap.setEnabled(".*", false);
        TapReport[] reports = Tap.getReport(".*");
        assertEquals(0, reports.length);
        Tap.enableInitial();
        reports = Tap.getReport(".*");
        assertEquals(2, reports.length);
        int mask = 0;
        for (TapReport report : reports) {
            if (report.getName().equals("c")) {
                mask |= 0x1;
            } else if (report.getName().equals("d")) {
                mask |= 0x2;
            } else {
                fail();
            }
        }
        assertEquals(0x3, mask);
    }
}
