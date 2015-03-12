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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.HKey;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.util.FilteringIterator;

import java.util.ArrayList;
import java.util.Iterator;

public class TableRowType extends AisRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return table.toString();
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return table.getColumnsIncludingInternal().size();
    }

    @Override
    public HKey hKey()
    {
        return table.hKey();
    }

    @Override
    public TInstance typeAt(int index) {
        return table.getColumnsIncludingInternal().get(index).getType();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        TableName tableName = table.getName();
        explainer.addAttribute(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(tableName.getSchemaName()));
        explainer.addAttribute(Label.TABLE_NAME, PrimitiveExplainer.getInstance(tableName.getTableName()));
        return explainer;
    }

    @Override
    public boolean fieldHasColumn(int field) {
        checkFieldRange(field);
        return true;
    }
    
    @Override 
    public Column fieldColumn(int field) {
        checkFieldRange(field);
        return table.getColumnsIncludingInternal().get(field);
    }
    
    // TableRowType interface
    @Override
    public Table table()
    {
        return table;
    }

    @Override
    public boolean hasTable() {
        return table != null;
    }

    public IndexRowType indexRowType(Index index)
    {
        return indexRowType(index.getIndexId());
    }

    public IndexRowType indexRowType(int indexID)
    {
        return indexRowTypes.get(indexID);
    }

    public void addIndexRowType(IndexRowType indexRowType)
    {
        Index index = indexRowType.index();
        int requiredEntries = index.getIndexId() + 1;
        while (indexRowTypes.size() < requiredEntries) {
            indexRowTypes.add(null);
        }
        indexRowTypes.set(index.getIndexId(), indexRowType);
    }

    public Iterable<IndexRowType> indexRowTypes() {
        return new Iterable<IndexRowType>() {
            @Override
            public Iterator<IndexRowType> iterator() {
                return new FilteringIterator<IndexRowType>(indexRowTypes.iterator(), false) {
                    @Override
                    protected boolean allow(IndexRowType item) {
                        return item != null;
                    }
                };
            }
        };
    }

    public TableRowType(Schema schema, Table table)
    {
        super(schema, table.getTableId());
        this.table = table;
        typeComposition(new SingleBranchTypeComposition(this, table));
    }

    // Object state

    private final Table table;
    // Type of indexRowTypes is ArrayList, not List, to make it clear that null values are permitted.
    private final ArrayList<IndexRowType> indexRowTypes = new ArrayList<>();
}
