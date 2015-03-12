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
 * A {@link com.foundationdb.util.tap.Tap} subclass that does nothing. This is the initial target of
 * every added {@link com.foundationdb.util.tap.Dispatch}.
 */
class Null extends Tap
{

    public Null(final String name)
    {
        super(name);
    }

    public void in()
    {
        // do nothing
    }

    public void out()
    {
        // do nothing
    }

    public long getDuration()
    {
        return 0;
    }

    public void reset()
    {
        // do nothing
    }

    public void appendReport(String label, final StringBuilder buffer)
    {
        // do nothing;
    }

    public String toString()
    {
        return "NullTap(" + name + ")";
    }

    public TapReport[] getReports()
    {
        return null;
    }
}
