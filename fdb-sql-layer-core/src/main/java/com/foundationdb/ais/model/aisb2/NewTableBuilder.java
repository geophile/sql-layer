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
package com.foundationdb.ais.model.aisb2;

import com.foundationdb.ais.model.TableName;

public interface NewTableBuilder extends NewAISBuilder {
    /**
     * Joins this table to another one, using the default schema
     * @param table the table to join to
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(String table);

    /**
     * Joins this table to another one.
     * @param tableName the name of the table to join to
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(TableName tableName);

    /**
     * Joins this table to another one.
     * @param schema the schema of the table to join to
     * @param table the table name of the table to join to
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(String schema, String table);

    /**
     * Joins this table to another one.
     * @param schema the schema of the table to join to
     * @param table the table name of the table to join to
     * @param fkName the name of the Akiban FK, <em>without</em> the {@code __akiban} prefix.
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(String schema, String table, String fkName);

    /**
     * Adds a non-nullable Int column
     * @param name the column's name
     * @return this
     */
    NewTableBuilder colInt(String name);

    /**
     * Adds an optionally nullable Int column
     * @param name the column's name
     * @param nullable whether the column is nullable
     * @return this
     */
    NewTableBuilder colInt(String name, boolean nullable);

    /**
     * Adds a non-nullable, sequence backed, auto-incrementing BY DEFAULT identity column
     * @param name the column's name
     * @param initialValue the START WITH value
     * @return this
     */
    NewTableBuilder autoIncInt(String name, int initialValue);

    /**
     * Adds a non-nullable, sequence backed, auto-incrementing identity column
     * @param name the column's name
     * @param initialValue the START WITH value
     * @param always ALWAYS if <code>true</code>, otherwise DEFAULT
     * @return this
     */
    NewTableBuilder autoIncInt(String name, int initialValue, boolean always);

    /**
     * Adds an optionally nullable boolean column
     * @param name the column's name
     * @param nullable whether the column is nullable
     * @return this
     */
    NewTableBuilder colBoolean(String name, boolean nullable);

    /**
     * Adds a non-nullable varchar with UTF-8 encoding
     * @param name the column's name
     * @param length the varchar's max length
     * @return this
     */
    NewTableBuilder colString(String name, int length);

    /**
     * Adds an optionally nullable varchar with UTF-8 encoding
     * @param name the column's name
     * @param length the varchar's max length
     * @param nullable whether the column is nullable
     * @return this
     */
    NewTableBuilder colString(String name, int length, boolean nullable);

    /**
     * Adds an optionally nullable varchar with a specified encoding
     * @param name the column's name
     * @param length the varchar's max length
     * @param nullable whether the column is nullable
     * @param charset the column's encoding
     * @return this
     */
    NewTableBuilder colString(String name, int length, boolean nullable, String charset);

    NewTableBuilder colDouble(String name);
    NewTableBuilder colDouble(String name, boolean nullable);
    
    NewTableBuilder colBigInt(String name);
    NewTableBuilder colBigInt(String name, boolean nullable);

    NewTableBuilder colVarBinary(String name, int length);
    NewTableBuilder colVarBinary(String name, int length, boolean nullable);
    
    NewTableBuilder colText(String name);
    NewTableBuilder colText(String name, boolean nullable);

    /*
    NewTableBuilder colTimestamp(String name);
    NewTableBuilder colTimestamp(String name, boolean nullable);
    */

    NewTableBuilder colSystemTimestamp(String name);
    NewTableBuilder colSystemTimestamp(String name, boolean nullable);

    /**
     * Adds a PK
     * @param columns the columns that are in the PK
     * @return this
     */
    NewTableBuilder pk(String... columns);

    /**
     * Adds a unique key
     * @param indexName the key's name
     * @param columns the columns in the key
     * @return this
     */
    NewTableBuilder uniqueKey(String indexName, String... columns);

    NewTableBuilder uniqueConstraint(String constraintName, String indexName, String... columns);

    /**
     * Adds a non-unique key
     * @param indexName the key's name
     * @param columns the columns in the key
     * @return this
     */
    NewTableBuilder key(String indexName, String... columns);
}
