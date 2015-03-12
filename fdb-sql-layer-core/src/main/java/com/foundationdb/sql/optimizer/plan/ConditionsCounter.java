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
package com.foundationdb.sql.optimizer.plan;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class ConditionsCounter<C> implements ConditionsCount<C> {
    public void clear() {
        counter.clear();
    }
    
    public int conditionsCounted() {
        return counter.size();
    }
    
    public Set<C> getCountedConditions() {
        return counter.keySet();
    }
    
    public void increment(C condition) {
        HowMany howMany = getCount(condition);
        switch (howMany) {
        case NONE:
            counter.put(condition, HowMany.ONE);
            break;
        case ONE:
            counter.put(condition, HowMany.MANY);
            break;
        case MANY:
            break;
        default:
            throw new AssertionError(howMany.name());
        }
    }

    @Override
    public HowMany getCount(C condition) {
        HowMany internalCount = counter.get(condition);
        return internalCount == null ? HowMany.NONE : internalCount;
    }

    public ConditionsCounter(int capacity) {
        counter = new HashMap<>(capacity);
    }

    // mapping of C -> [0, 1, >1 ]
    private Map<C,HowMany> counter;
}
