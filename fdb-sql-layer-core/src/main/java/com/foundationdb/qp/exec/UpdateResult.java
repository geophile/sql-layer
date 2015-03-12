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
package com.foundationdb.qp.exec;

public interface UpdateResult {
    /**
     * <p>The number of rows that were touched by this query, including those which were not modified.</p>
     *
     * <p>For instance, if you had {@code UPDATE my_table SET name='Robert'}, this would be the total number of
     * rows in {@code my_table}.</p>
     * @return the number of rows touched by the query, including for read-only scanning
     */
    int rowsTouched();

    /**
     * <p>The number of rows that were modified or deleted by this query.</p>
     *
     * <p>For instance, if you had {@code UPDATE my_table SET name='Robert'}, this would be the total number of
     * rows in {@code my_table} which did not originally have {@code name='Robert'} (and which therefore had to
     * be updated).</p>
     * @return the number of rows touched by the query, including for read-only scanning
     */
    int rowsModified();
}
