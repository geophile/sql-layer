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
package com.foundationdb.server.types;

import java.util.Arrays;

public final class TInstanceGenerator {
    public TInstance setNullable(boolean isNullable) {
        switch (attrs.length) {
        case 0:
            return tclass.instance(isNullable);
        case 1:
            return tclass.instance(attrs[0], isNullable);
        case 2:
            return tclass.instance(attrs[0], attrs[1], isNullable);
        case 3:
            return tclass.instance(attrs[0], attrs[1], attrs[2], isNullable);
        case 4:
            return tclass.instance(attrs[0], attrs[1], attrs[2], attrs[3], isNullable);
        default:
            throw new AssertionError("too many attrs!: " + Arrays.toString(attrs) + " with " + tclass);
        }
    }

    int[] attrs() {
        return Arrays.copyOf(attrs, attrs.length);
    }

    TClass tClass() {
        return tclass;
    }

    public String toString(boolean useShorthand) {
        return setNullable(true).toStringIgnoringNullability(useShorthand);
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public TInstanceGenerator(TClass tclass, int... attrs) {
        this.tclass = tclass;
        this.attrs = Arrays.copyOf(attrs, attrs.length);
    }
    private final TClass tclass;

    private final int[] attrs;
}
