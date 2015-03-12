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
package com.foundationdb.server.types.service;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.service.InstanceFinder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

final class KeyComparableRegistry {

    public KeyComparableRegistry(InstanceFinder finder) {
        Collection<? extends TKeyComparable> keyComparables = finder.find(TKeyComparable.class);
        keyComparablesMap = new HashMap<>(keyComparables.size());
        for (TKeyComparable keyComparable : keyComparables) {
            TwoElemSet key = new TwoElemSet(keyComparable.getLeftTClass(), keyComparable.getRightTClass());
            keyComparablesMap.put(key, keyComparable);
        }
    }

    private final Map<TwoElemSet, TKeyComparable> keyComparablesMap;

    public TKeyComparable getClass(TClass left, TClass right) {
        return keyComparablesMap.get(new TwoElemSet(left, right));
    }

    private static final class TwoElemSet {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TwoElemSet that = (TwoElemSet) o;

            if (a == that.a)
                return b == that.b;
            else
                return a == that.b && b == that.a;
        }

        @Override
        public int hashCode() {
            return a.hashCode() + b.hashCode();
        }

        private TwoElemSet(TClass a, TClass b) {
            this.a = a;
            this.b = b;
        }

        private final TClass a, b;
    }
}
