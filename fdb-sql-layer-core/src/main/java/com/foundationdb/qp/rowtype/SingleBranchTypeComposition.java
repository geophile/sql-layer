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
package com.foundationdb.qp.rowtype;


import com.foundationdb.ais.model.Table;

import java.util.*;

public class SingleBranchTypeComposition extends TypeComposition
{
    /**
     * Indicates whether this is an ancestor of that: this is identical to that, or:
     * - the tables comprising this and that are disjoint, and
     * - the rootmost table of that has an ancestor among the tables of this.
     * @param typeComposition
     * @return true if this is an ancestor of that, false otherwise.
     */
    public boolean isAncestorOf(TypeComposition typeComposition)
    {
        Boolean ancestor;
        SingleBranchTypeComposition that = (SingleBranchTypeComposition) typeComposition;
        if (this == that) {
            ancestor = Boolean.TRUE;
        } else {
            ancestor = ancestorOf.get(that.rowType);
            if (ancestor == null) {
                // Check for tables in common
                ancestor = Boolean.TRUE;
                for (Table table : that.tables) {
                    if (this.tables.contains(table)) {
                        ancestor = Boolean.FALSE;
                    }
                }
                if (ancestor) {
                    ancestor = levelsApart(that) > 0;
                }
                ancestorOf.put(that.rowType, ancestor);
            }
        }
        return ancestor;
    }

    /**
     * Indicates whether this is a parentof that: this is not identical to that, and:
     * - the tables comprising this and that are disjoint, and
     * - the rootmost table's parent is among the tables of this.
     * @param typeComposition
     * @return true if this is an ancestor of that, false otherwise.
     */
    public boolean isParentOf(TypeComposition typeComposition)
    {
        boolean ancestor;
        SingleBranchTypeComposition that = (SingleBranchTypeComposition) typeComposition;
        if (this == that) {
            ancestor = false;
        } else {
            ancestor = isAncestorOf(that);
            if (ancestor) {
                ancestor = levelsApart(that) > 0;
            }
        }
        return ancestor;
    }

    public SingleBranchTypeComposition(RowType rowType, Table table)
    {
        this(rowType, Arrays.asList(table));
    }

    public SingleBranchTypeComposition(RowType rowType, Collection<Table> tables)
    {
        super(rowType, tables);
    }

    // For use by this class

    // If this is an ancestor of that, then the return value is the number of generations separating the two.
    // (parent = 1). If this is not an ancestor of that, return -1.
    public int levelsApart(SingleBranchTypeComposition that)
    {
        // Find rootmost table in that
        Table thatRoot = that.tables.iterator().next();
        while (thatRoot.getParentTable() != null && that.tables.contains(thatRoot.getParentTable())) {
            thatRoot = thatRoot.getParentTable();
        }
        // this is an ancestor of that if that's rootmost table has an ancestor in this.
        int generationsApart = 0;
        Table thatAncestor = thatRoot;
        boolean ancestor = false;
        while (thatAncestor != null && !ancestor) {
            thatAncestor = thatAncestor.getParentTable();
            ancestor = this.tables.contains(thatAncestor);
            generationsApart++;
        }
        return ancestor ? generationsApart : -1;
    }

    // Object state

    private final Map<RowType, Boolean> ancestorOf = new HashMap<>();
}
