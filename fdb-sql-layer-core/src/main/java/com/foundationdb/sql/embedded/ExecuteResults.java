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
package com.foundationdb.sql.embedded;

import com.foundationdb.qp.operator.RowCursor;

import java.sql.ResultSet;
import java.util.Queue;

class ExecuteResults
{
    private int updateCount;
    private RowCursor cursor;
    private Queue<ResultSet> additionalResultSets;

    /** No results. */
    public ExecuteResults() {
        this.updateCount = -1;
    }

    /** Ordinary select result. 
     * Transaction remains open while it is visited.
     */
    public ExecuteResults(RowCursor cursor) {
        this.updateCount = -1;
        this.cursor = cursor;
    }

    /** Update result, possibly with returned keys. 
     * These keys are already copied in order to get update count correct.
     */
    public ExecuteResults(int updateCount, RowCursor generatedKeys) {
        this.updateCount = updateCount;
        this.cursor = generatedKeys;
    }

    /** Stored procedure returning result sets of unknown provenance. */
    public ExecuteResults(Queue<ResultSet> resultSets) {
        this.updateCount = -1;
        this.additionalResultSets = resultSets;
    }
    
    public int getUpdateCount() {
        return updateCount;
    }
    
    public RowCursor getCursor() {
        return cursor;
    }

    public boolean hasResultSet() {
        return (updateCount < 0);
    }
    
    public Queue<ResultSet> getAdditionalResultSets() {
        return additionalResultSets;
    }
    
}
