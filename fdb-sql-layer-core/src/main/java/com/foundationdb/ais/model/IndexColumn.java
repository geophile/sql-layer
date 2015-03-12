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
package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;

import com.foundationdb.server.collation.AkCollator;

public class IndexColumn implements Visitable
{
    // IndexColumn interface

    @Override
    public String toString()
    {
        return "IndexColumn(" + column.getName() + ")";
    }

    public Index getIndex()
    {
        return index;
    }

    public Column getColumn()
    {
        return column;
    }

    public Integer getPosition()
    {
        return position;
    }

    public Integer getIndexedLength()
    {
        return indexedLength;
    }

    public Boolean isAscending()
    {
        return ascending;
    }

    /** Can this index column be used as part of a <em>covering</em> index? */
    public boolean isRecoverable() {
        AkCollator collator = column.getCollator();
        if (collator == null)
            return true;
        else
            return collator.isRecoverable();
    }

    // Visitable

    /** Visit this instance. */
    @Override
    public void visit(Visitor visitor) {
        visitor.visit(this);
    }

    public static IndexColumn create(Index index,
                                     Column column,
                                     Integer position,
                                     Boolean ascending,
                                     Integer indexedLength) {
        index.checkMutability();
        AISInvariants.checkNullField(column, "IndexColumn", "column", "Column");
        AISInvariants.checkDuplicateColumnsInIndex(index, column.getColumnar().getName(), column.getName());
        IndexColumn indexColumn = new IndexColumn(index, column, position, ascending, indexedLength);
        index.addColumn(indexColumn);
        return indexColumn;
    }

    /**
     * Create an independent copy of an existing IndexColumn.
     * @param index Destination Index.
     * @param column Associated Column.
     * @param indexColumn IndexColumn to copy.
     * @return The new copy of the IndexColumn.
     */
    public static IndexColumn create(Index index, Column column, IndexColumn indexColumn, int position) {
        return create(index, column, position, indexColumn.ascending, indexColumn.indexedLength);
    }
    
    IndexColumn(Index index, Column column, Integer position, Boolean ascending, Integer indexedLength)
    {
        this.index = index;
        this.column = column;
        this.position = position;
        this.ascending = ascending;
        this.indexedLength = indexedLength;
    }
    
    // State

    private final Index index;
    private final Column column;
    private final Integer position;
    private final Boolean ascending;
    private final Integer indexedLength;
}
