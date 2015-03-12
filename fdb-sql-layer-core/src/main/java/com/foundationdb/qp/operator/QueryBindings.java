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
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.BloomFilter;
import com.foundationdb.qp.util.HashTable;

/** The bindings associated with the execution of a query.
 * This includes query parameters (? markers) as well as current values for iteration.
 * More than one QueryBindings may be active at the same time if
 * iteration is being done in parallel for pipelining.
 */
public interface QueryBindings
{
    public ValueSource getValue(int index);

    public void setValue(int index, ValueSource value);

    /**
     * Gets the row bound to the given index.
     * @param index the index to look up
     * @return the row at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public Row getRow(int index);

    /**
     * Bind a row to the given index.
     * @param index the index to set
     * @param row the row to assign
     */
    public void setRow(int index, Row row);

    /**
     * Gets the hKey bound to the given index.
     * @param index the index to look up
     * @return the hKey at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public HKey getHKey(int index);

    /**
     * Bind an hkey to the given index.
     * @param index the index to set
     * @param hKey the hKey to assign
     */
    public void setHKey(int index, HKey hKey);

    /**
     * Gets the bloom filter bound to the given index.
     * @param index the index to look up
     * @return the bloom filter at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public BloomFilter getBloomFilter(int index);

    /**
     * Bind a bloom filter to the given index.
     * @param index the index to set
     * @param filter the bloom filter to assign
     */
    public void setBloomFilter(int index, BloomFilter filter);

    /**
     * Gets the hash join table bound to the given index.
     * @param index the index to look up
     * @return the hash join table at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public HashTable getHashTable(int index);

    /**Bind a hash table hash join table to the given index.
     * @param index the index to set
     * @param hashTable the hash join table to assign
     */
    public void setHashTable(int index, HashTable hashTable);

    /**
     * Clear all bindings.
     */
    public void clear();

    /**
     * Get the parent from which undefined bindings inherit.
     */
    public QueryBindings getParent();

    /**
     * Is this bindings an descendant of the given bindings?
     */
    public boolean isAncestor(QueryBindings ancestor);

    /**
     * Get the inheritance depth.
     */
    public int getDepth();

    /**
     * Make a new set of bindings inheriting from this one.
     */
    public QueryBindings createBindings();
}
