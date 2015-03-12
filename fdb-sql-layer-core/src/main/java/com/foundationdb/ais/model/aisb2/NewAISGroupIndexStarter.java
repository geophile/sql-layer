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

public interface NewAISGroupIndexStarter {

    /**
     * Invokes {@link #on(String, String, String)} with the default schema
     * @param table the table name
     * @param column  the schema name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder on(String table, String column);

    /**
     * Builds the first column of a group index.
     * This method sets the group for the upcoming group index; all subsequent calls to
     * {@link NewAISGroupIndexBuilder#and(String, String, String)} must reference tables also in this group.
     * @param schema the table's schema
     * @param table the table's name
     * @param column the column name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder on(String schema, String table, String column);
}
