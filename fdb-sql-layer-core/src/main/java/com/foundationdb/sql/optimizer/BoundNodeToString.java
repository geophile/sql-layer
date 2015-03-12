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
package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.unparser.NodeToString;
import com.foundationdb.sql.parser.*;

import com.foundationdb.sql.StandardException;

import com.foundationdb.ais.model.Table;

/**
 * A version of NodeToString that shows qualified names after binding.
 */
public class BoundNodeToString extends NodeToString {
    private boolean useBindings = false;

    public boolean isUseBindings() {
        return useBindings;
    }
    public void setUseBindings(boolean useBindings) {
        this.useBindings = useBindings;
    }

    protected String tableName(TableName node) throws StandardException {
        if (useBindings) {
            Table table = (Table)node.getUserData();
            if (null != table)
                return table.getName().toString();
        }
        return node.getFullTableName();
    }

    protected String columnReference(ColumnReference node) throws StandardException {
        if (useBindings) {
            ColumnBinding columnBinding = (ColumnBinding)node.getUserData();
            if (columnBinding != null) {
                FromTable fromTable = columnBinding.getFromTable();
                return ((fromTable.getCorrelationName() != null) ?
                        fromTable.getCorrelationName() :
                        toString(fromTable.getTableName())) +
                    "." + node.getColumnName();
            }
        }
        return node.getSQLColumnName();
    }
}
