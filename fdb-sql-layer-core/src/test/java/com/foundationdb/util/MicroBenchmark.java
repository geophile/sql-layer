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

import java.sql.SQLException;

public abstract class MicroBenchmark
{
    public void beforeAction() throws Exception
    {}

    public void afterAction() throws Exception
    {}

    public abstract void action() throws Exception;

    public String name()
    {
        return getClass().getSimpleName();
    }

    public final double run() throws Exception
    {
        while (!converged()) {
            beforeAction();
            long start = System.nanoTime();
            action();
            long stop = System.nanoTime();
            afterAction();
            history[cycles++ % history.length] = stop - start;
        }
        return historyAverage;
    }

    public final void profile() throws Exception
    {
        while (true) {
            beforeAction();
            action();
            afterAction();
        }
    }

    protected MicroBenchmark(int historySize, double maxVariation)
    {
        this.maxVariation = maxVariation;
        this.history = new long[historySize];
    }

    private boolean converged()
    {
        boolean converged = false;
        if (cycles >= history.length) {
            long sum = 0;
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (int i = 0; i < history.length; i++) {
                sum += history[i];
                min = Math.min(min, history[i]);
                max = Math.max(max, history[i]);
            }
            historyAverage = (double) sum / history.length;
            converged =
                min >= historyAverage * (1 - maxVariation) &&
                max <= historyAverage * (1 + maxVariation);
        }
        if (converged) {
            // In case this benchmark is rerun
            cycles = 0;
        }
        return converged;
    }

    private final double maxVariation;
    private long[] history;
    private double historyAverage;
    private int cycles = 0;
}
