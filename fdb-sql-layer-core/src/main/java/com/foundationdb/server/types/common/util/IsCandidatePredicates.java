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
package com.foundationdb.server.types.common.util;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.google.common.base.Predicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public final class IsCandidatePredicates {

    public static Predicate<List<? extends TPreptimeValue>> contains(TClass tClass) {
        final TClass tClassFinal = tClass;
        return new Predicate<List<? extends TPreptimeValue>>() {
            @Override
            public boolean apply(List<? extends TPreptimeValue> input) {
                for (int i = 0, size=input.size(); i < size; ++i) {
                    TInstance type = input.get(i).type();
                    if ((type != null) && (type.typeClass() == tClassFinal))
                        return true;
                }
                return false;
            }
        };
    }

    public static Predicate<List<? extends TPreptimeValue>> allTypesKnown =
            new Predicate<List<? extends TPreptimeValue>>() {
                @Override
                public boolean apply(List<? extends TPreptimeValue> inputs) {
                    for (int i = 0, size=inputs.size(); i < size; ++i) {
                        if (inputs.get(i).type() == null)
                            return false;
                    }
                    return true;
                }
            };

    public static Predicate<List<? extends TPreptimeValue>> containsOnly(Collection<? extends TClass> tClasses) {

        final Collection<TClass> asSet = new HashSet<>(tClasses.size());
        asSet.addAll(tClasses);
        return new Predicate<List<? extends TPreptimeValue>>() {
            @Override
            public boolean apply(List<? extends TPreptimeValue> inputs) {
                for (int i = 0, size = inputs.size(); i < size; ++i) {
                    TInstance type = inputs.get(i).type();
                    if (type == null || (!asSet.contains(type.typeClass())))
                        return false;
                }
                return true;
            }
        };
    }

    private IsCandidatePredicates() {}
}
