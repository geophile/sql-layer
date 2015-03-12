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

import com.foundationdb.ais.model.HKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;

public class HKeyRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return "HKey";
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return nFields;
    }

    @Override
    public TInstance typeAt(int index) {
        return hKey().column(index).getType();
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }
    
    @Override
    public Table table() {
        return hKey.table();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        TableName tableName = hKey.table().getName();
        explainer.addAttribute(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(tableName.getSchemaName()));
        explainer.addAttribute(Label.TABLE_NAME, PrimitiveExplainer.getInstance(tableName.getTableName()));
        return explainer;
    }

    // HKeyRowType interface
    
    public HKeyRowType(Schema schema, int typeId, HKey hKey)
    {
        super(schema, typeId);
        this.hKey = hKey;
        this.nFields = hKey.nColumns();
    }

    // Object state

    private final int nFields;
    private HKey hKey;
}
