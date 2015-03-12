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
package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.util.BloomFilter;
import com.foundationdb.qp.util.HashTable;
import com.foundationdb.util.SparseArray;

public class SparseArrayQueryBindings implements QueryBindings
{
    private final SparseArray<Object> bindings = new SparseArray<>();
    private final QueryBindings parent;
    private final int depth;

    public SparseArrayQueryBindings() {
        this.parent = null;
        this.depth = 0;
    }

    public SparseArrayQueryBindings(QueryBindings parent) {
        this.parent = parent;
        this.depth = parent.getDepth() + 1;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        str.append('(');
        bindings.describeElements(str);
        if (parent != null) {
            str.append(", ");
            str.append(parent);
        }
        str.append(')');
        return str.toString();
    }

    /* QueryBindings interface */

    @Override
    public ValueSource getValue(int index) {
        if (bindings.isDefined(index)) {
            return (ValueSource)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getValue(index);
        }
        else {
            throw new BindingNotSetException(index);
        }
    }

    /*
     * (non-Javadoc)
     * @see com.foundationdb.qp.operator.QueryContext#setValue(int, com.foundationdb.server.types.value.ValueSource)
     * This makes a copy of the ValueSource value, rather than simply
     * storing the reference. The assumption is the ValueSource parameter
     * will be reused by the caller as rows are processed, so the QueryContext
     * needs to keep a copy of the underlying value.
     *
     */
    @Override
    public void setValue(int index, ValueSource value) {
        Value holder = null;
        if (bindings.isDefined(index)) {
            holder = (Value)bindings.get(index);
            if (holder.getType() != value.getType())
                holder = null;
        }
        if (holder == null) {
            holder = new Value(value.getType());
            bindings.set(index, holder);
        }
        ValueTargets.copyFrom(value, holder);
    }
    
    @Override
    public Row getRow(int index) {
        if (bindings.isDefined(index)) {
            return (Row)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getRow(index);
        }
        else {
            throw new BindingNotSetException(index);
        }
    }

    @Override
    public void setRow(int index, Row row)
    {
        bindings.set(index, row);
    }

    @Override
    public HKey getHKey(int index) {
        if (bindings.isDefined(index)) {
            return (HKey)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getHKey(index);
        }
        else {
            throw new BindingNotSetException(index);
        }
    }

    @Override
    public void setHKey(int index, HKey hKey)
    {
        bindings.set(index, hKey);
    }

    @Override
    public BloomFilter getBloomFilter(int index) {
        if (bindings.isDefined(index)) {
            return (BloomFilter)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getBloomFilter(index);
        }
        else {
            throw new BindingNotSetException(index);
        }
    }

    @Override
    public void setBloomFilter(int index, BloomFilter filter) {
        bindings.set(index, filter);
    }

    @Override
    public HashTable getHashTable(int index){
        if (bindings.isDefined(index)) {
            return (HashTable)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getHashTable(index);
        }
         else {
            throw new BindingNotSetException(index);
        }
    }

    @Override
    public void setHashTable(int index, HashTable hashTable) {
        bindings.set(index, hashTable);
    }

    @Override
    public void clear() {
        bindings.clear();
    }

    @Override
    public QueryBindings getParent() {
        return parent;
    }

    @Override
    public boolean isAncestor(QueryBindings ancestor) {
        for (QueryBindings descendant = this; descendant != null; descendant = descendant.getParent()) {
            if (descendant == ancestor) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getDepth() {
        return depth;
    }

    @Override
    public QueryBindings createBindings() {
        return new SparseArrayQueryBindings(this);
    }
}
