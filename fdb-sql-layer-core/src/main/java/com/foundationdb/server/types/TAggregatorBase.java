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

import com.foundationdb.util.BitSets;
import com.google.common.base.Predicate;

import java.util.Collections;
import java.util.List;

public abstract class TAggregatorBase implements TAggregator {

    @Override
    public String id() {
        return getClass().getName();
    }

    @Override
    public int[] getPriorities() {
        return new int[] { 1 };
    }

    @Override
    public String[] registeredNames() {
        return new String[] { displayName() };
    }

    @Override
    public final String displayName() {
        return name;
    }

    @Override
    public List<TInputSet> inputSets() {
        return Collections.singletonList(
                new TInputSet(inputClass, BitSets.of(0), false, inputClass == null, null));
    }

    @Override
    public InputSetFlags exactInputs() {
        return InputSetFlags.ALL_OFF;
    }

    @Override
    public final String toString() {
        return displayName();
    }

    protected TClass inputClass() {
        return inputClass;
    }

    protected TAggregatorBase(String name, TClass inputClass) {
        this.name = name;
        this.inputClass = inputClass;
    }

    private final String name;
    private final TClass inputClass;
}
