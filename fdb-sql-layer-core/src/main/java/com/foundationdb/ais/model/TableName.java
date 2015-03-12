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

public class TableName implements Comparable<TableName>
{
    public final static String INFORMATION_SCHEMA = "information_schema";
    public final static String SECURITY_SCHEMA = "security_schema";
    public final static String SQLJ_SCHEMA = "sqlj";
    public final static String SYS_SCHEMA = "sys";

    private final String schemaName;
    private final String tableName;

    public TableName(String schemaName, String tableName)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public static TableName create(String schemaName, String tableName)
    {
        return new TableName(schemaName, tableName);
    }

    /** Parse a qualified string (e.g. test.foo) into a TableName. */
    public static TableName parse(String defaultSchema, String s)
    {
        String[] parts = Strings.parseQualifiedName(s, 2);
        return new TableName(parts[0].isEmpty() ? defaultSchema : parts[0], parts[1]);
    }
    
    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getDescription()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getSchemaName());
        buffer.append(".");
        buffer.append(getTableName());
        return buffer.toString();
    }

    public String toStringEscaped() {
        return String.format("%s.%s", Strings.escapeIdentifier(schemaName), Strings.escapeIdentifier(tableName));
    }

    public boolean inSystemSchema() {
        return inSystemSchema(schemaName);
    }

    public static boolean inSystemSchema(String schemaName) {
        return INFORMATION_SCHEMA.equals(schemaName) ||
                SECURITY_SCHEMA.equals(schemaName) ||
                SQLJ_SCHEMA.equals(schemaName) ||
                SYS_SCHEMA.equals(schemaName);
    }

    @Override
    public String toString()
    {
        return getDescription();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (! (obj instanceof TableName)) {
            return false;
        }
        TableName o = (TableName) obj;

        return equals(o.getSchemaName(), o.getTableName());
    }
    
    public boolean equals(String schema, String table)
    {
        return getSchemaName().equals(schema) &&
                getTableName().equals(table);
    }

    @Override
    public int hashCode()
    {
        return getSchemaName().hashCode() +
               getTableName().hashCode();
    }

    @Override
    public int compareTo(TableName that)
    {
        int c = this.getSchemaName().compareTo(that.getSchemaName());
        if (c == 0) {
            c = this.getTableName().compareTo(that.getTableName());
        }
        return c;
    }


    /**
     * Writes this table's escaped name to the builder. If the table's schema matches (case sensitive) the given
     * unlessSchema that portion of the name will <em>not</em> be included in the returned String. Otherwise, it will
     * be. If unlessSchema is null, this will always print the fully qualified name.
     * @param builder the builder to write the escaped name to
     * @param unlessSchema if matches this TableName's schema, print only the table's escaped name (not fully qualified)
     * @return the builder you passed in
     */
    private StringBuilder escape(StringBuilder builder, String unlessSchema) {
        if (unlessSchema == null || (!schemaName.equals(unlessSchema))) {
            if (needsEscaping(schemaName)) {
                escape(schemaName, builder);
            }
            else {
                builder.append(schemaName);
            }
            builder.append('.');
        }

        if (needsEscaping(tableName)) {
            escape(tableName, builder);
        }
        else {
            builder.append(tableName);
        }

        return builder;
    }

    /**
     * Prints this table's escaped, fully qualified name.
     * @return the escaped, fully qualified name
     */
    public String escaped() {
        return escaped(null);
    }

    /**
     * Returns this table's escaped name. If the table's schema matches (case sensitive) the given unlessSchema,
     * that portion of the name will <em>not</em> be included in the returned String. Otherwise, it will be. If
     * unlessSchema is null, this will always print the fully qualified name.
     * @param unlessSchema if matches this TableName's schema, print only the table's escaped name (not fully qualified)
     * @return the escaped name
     */
    public String escaped(String unlessSchema) {
        return escape(new StringBuilder(), unlessSchema).toString();
    }

    private static boolean needsEscaping(String name) {
        char[] chars = new char[ name.length() ];
        name.getChars(0, name.length(), chars, 0);
        for (char c : chars) {
            if (! (Character.isLetterOrDigit(c) || c == '_')) {
                return true;
            }
        }
        return false;
    }

    private static StringBuilder escape(String name, StringBuilder out) {
        out.append('`').append(name.replace("`", "``")).append('`');
        return out;
    }
}
