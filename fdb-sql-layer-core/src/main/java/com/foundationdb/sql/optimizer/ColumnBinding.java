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

import com.foundationdb.sql.parser.FromTable;
import com.foundationdb.sql.parser.ResultColumn;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.DataTypeDescriptor;

import com.foundationdb.ais.model.Column;

/**
 * A column binding: stored in the UserData of a ColumnReference and
 * referring to a column from one of the tables in some FromList,
 * either as a DDL column (which may not be mentioned in any select
 * list) or a result column in a subquery.
 */
public class ColumnBinding 
{
    private FromTable fromTable;
    private Column column;
    private ResultColumn resultColumn;
    private boolean nullable;
        
    public ColumnBinding(FromTable fromTable, Column column, boolean nullable) {
        this.fromTable = fromTable;
        this.column = column;
        this.nullable = nullable;
    }
    public ColumnBinding(FromTable fromTable, ResultColumn resultColumn) {
        this.fromTable = fromTable;
        this.resultColumn = resultColumn;
    }

    public FromTable getFromTable() {
        return fromTable;
    }

    public Column getColumn() {
        return column;
    }

    /** Is the column nullable by virtue of its table being in an outer join? */
    public boolean isNullable() {
        return nullable;
    }

    public ResultColumn getResultColumn() {
        return resultColumn;
    }

    public DataTypeDescriptor getType() throws StandardException {
        if (resultColumn != null) {
            return resultColumn.getType();
        }
        else {
            return getType(column, nullable);
        }
    }
    
    public static DataTypeDescriptor getType(Column column, boolean nullable)
            throws StandardException {
        DataTypeDescriptor type = column.getType().dataTypeDescriptor();
        if (nullable)
            type = type.getNullabilityType(nullable);
        return type;
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        if (resultColumn != null) {
            result.append(resultColumn.getClass().getName());
            result.append('@');
            result.append(Integer.toHexString(resultColumn.hashCode()));
        }
        else
            result.append(column);
        if (fromTable != null) {
            result.append(" from ");
            result.append(fromTable.getClass().getName());
            result.append('@');
            result.append(Integer.toHexString(fromTable.hashCode()));
        }
        return result.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ColumnBinding)) return false;
        ColumnBinding other = (ColumnBinding)obj;
        return ((fromTable == other.fromTable) &&
                (column == other.column) &&
                (resultColumn == other.resultColumn));
    }

    @Override
    public int hashCode() {
        int hash = fromTable.hashCode();
        if (column != null)
            hash += column.hashCode();
        if (resultColumn != null)
            hash += resultColumn.hashCode();
        return hash;
    }

}
