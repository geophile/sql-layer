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
import java.util.Collections;
import java.util.List;

/**
 * A very thin shim around List<TClass></TClass>. Mostly there so that the call sites don't have to worry about
 * generics. This is especially useful for the reflective registration, where it's easier to search for a TCastPath
 * than fora {@code Collection&lt;? extends List&lt;? extends TClass&gt;&gt;}.
 */
public final class TCastPath {

    public static TCastPath create(TClass first, TClass second, TClass third, TClass... rest) {
        TClass[] all = new TClass[rest.length + 3];
        all[0] = first;
        all[1] = second;
        all[2] = third;
        System.arraycopy(rest, 0, all, 3, rest.length);
        List<? extends TClass> list = Arrays.asList(all);
        return new TCastPath(list);
    }

    private TCastPath(List<? extends TClass> list) {
        if (list.size() < 3)
            throw new IllegalArgumentException("cast paths must contain at least three elements: " + list);
        this.list = Collections.unmodifiableList(list);
    }

    public List<? extends TClass> getPath() {
        return list;
    }

    private final List<? extends TClass> list;
}
