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

import java.util.*;

public abstract class BaseDuplicatable implements Duplicatable, Cloneable
{
    @Override
    public final Duplicatable duplicate() {
        return duplicate(new DuplicateMap());
    }

    protected boolean maintainInDuplicateMap() {
        return false;
    }

    @Override
    public Duplicatable duplicate(DuplicateMap map) {
        BaseDuplicatable copy;
        try {
            if (maintainInDuplicateMap()) {
                copy = map.get(this);
                if (copy != null)
                    return copy;
                copy = (BaseDuplicatable)clone();
                map.put(this, copy);
            }
            else
                copy = (BaseDuplicatable)clone();
        }
        catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
        copy.deepCopy(map);
        return copy;
    }

    /** Deep copy all the fields, using the given map. */
    protected void deepCopy(DuplicateMap map) {
    }

    @SuppressWarnings("unchecked")
    protected static <T extends Duplicatable> List<T> duplicateList(Collection<? extends T> list,
                                                                    DuplicateMap map) {
        List<T> copy = new ArrayList<>(list.size());
        for (T elem : list) {
            copy.add((T)elem.duplicate(map));
        }
        return copy;
    }

    @SuppressWarnings("unchecked")
    protected static <T extends Duplicatable> Set<T> duplicateSet(Collection<? extends T> set,
                                                                  DuplicateMap map) {
        Set<T> copy = new HashSet<>(set.size());
        for (T elem : set) {
            copy.add((T)elem.duplicate(map));
        }
        return copy;
    }

}
