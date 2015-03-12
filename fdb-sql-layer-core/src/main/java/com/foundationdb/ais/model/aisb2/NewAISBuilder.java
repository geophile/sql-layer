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

public interface NewAISBuilder extends NewAISProvider {
    /**
     * Sets the default schema
     * @param schema the new default schema name; like SQL's {@code USING}.
     * @return {@code this}
     */
    NewAISBuilder defaultSchema(String schema);

    /**
     * Starts creating a new table using the default schema.
     * @param table the table's name
     * @return the new table's builder
     */
    NewTableBuilder table(String table);

    /**
     * Starts creating a new table using the given schema
     * @param schema the new table's schema
     * @param table the new table's table name
     * @return the new table's builder
     */
    NewTableBuilder table(String schema, String table);

    NewTableBuilder table(TableName tableName);
    
    /**
     * Returns the NewTableBuilder for the table being built
     * @return
     */
    NewTableBuilder getTable();
    NewTableBuilder getTable(TableName table);
   
    /**
     * create a new sequence
     */
    NewAISBuilder sequence (String name);
    NewAISBuilder sequence (String name, long start, long increment, boolean isCycle);

    /**
     * create a new view 
     * @param view name of view
     * @return
     */
    NewViewBuilder view(String view);

    NewViewBuilder view(String schema, String view);

    NewViewBuilder view(TableName viewName);

    /**
     * create a new procedure 
     * @param procedure name of procedure
     * @return
     */
    NewRoutineBuilder procedure(String procedure);

    NewRoutineBuilder procedure(String schema, String procedure);

    NewRoutineBuilder procedure(TableName procedureName);

    /**
     * create a new SQL/J jar 
     * @param jarName name of jar file
     * @return
     */
    NewSQLJJarBuilder sqljJar(String jarName);

    NewSQLJJarBuilder sqljJar(String schema, String jarName);

    NewSQLJJarBuilder sqljJar(TableName name);
}
