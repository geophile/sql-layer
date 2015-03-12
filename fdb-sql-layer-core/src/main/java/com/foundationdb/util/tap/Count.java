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

/**
 * A Tap subclass that counts calls to {@link #in()} and {@link #out()}.
 * Generally this is faster than {@link TimeAndCount} because the system
 * clock is not read.
 */
class Count extends Tap
{
    // Object interface

    public String toString()
    {
        return String.format("%s inCount=%,d outCount=%,d", name, inCount, outCount);
    }

    // Tap interface

    public void in()
    {
        justEnabled = false;
        checkNesting();
        inCount++;
    }

    public void out()
    {
        if (justEnabled) {
            justEnabled = false;
        } else {
            outCount++;
            checkNesting();
        }
    }

    public long getDuration()
    {
        return 0;
    }

    public void reset()
    {
        inCount = 0;
        outCount = 0;
        justEnabled = true;
    }

    public void appendReport(String label, StringBuilder buffer)
    {
        buffer.append(String.format("%s %20s inCount=%,10d outCount=%,10d", label, name, inCount, outCount));
    }

    public TapReport[] getReports()
    {
        return new TapReport[]{new TapReport(name, inCount, outCount, 0)};
    }

    // Count interface

    public Count(String name)
    {
        super(name);
    }

    // Object state

    private volatile boolean justEnabled = false;
}
