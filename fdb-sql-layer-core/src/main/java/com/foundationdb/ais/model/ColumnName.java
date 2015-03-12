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

import com.foundationdb.util.Strings;

public class ColumnName
{
    private final TableName tableName;
    private final String columnName;

    public ColumnName(String schema, String table, String columnName) {
        this(new TableName(schema, table), columnName);
    }

    public ColumnName(Column column) {
        this(column.getTable().getName(), column.getName());
    }

    public ColumnName(TableName tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    /** Parse a qualified string (e.g. test.foo.id) into a ColumnName. */
    public static ColumnName parse(String defaultSchema, String s) {
        String[] parts = Strings.parseQualifiedName(s, 3);
        return new ColumnName(parts[0].isEmpty() ? defaultSchema : parts[0], parts[1], parts[2]);
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getName() {
        return columnName;
    }

    public String toStringEscaped() {
        return String.format("%s.%s", tableName.toStringEscaped(), Strings.escapeIdentifier(columnName));
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", tableName.getSchemaName(), tableName.getTableName(), columnName);
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        if(o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnName that = (ColumnName)o;
        return columnName.equals(that.columnName) && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        int result = tableName.hashCode();
        result = 31 * result + columnName.hashCode();
        return result;
    }
}
