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

import com.foundationdb.ais.util.TableChange;

import java.util.List;

/** Attached to a <code>Table</code> on which <code>ALTER</code> has been performed
 * by <a href="http://www.percona.com/doc/percona-toolkit/2.1/pt-online-schema-change.html">pt-online-schema-change.html</a>. 
 * The same alter will be done to the <code>originalName</code> when a
 * <code>RENAME</code> is requested after all the row copying.
 */
public class PendingOSC
{
    private String originalName, currentName;
    private List<TableChange> columnChanges, indexChanges;

    public PendingOSC(String originalName, List<TableChange> columnChanges, List<TableChange> indexChanges) {
        this.originalName = originalName;
        this.columnChanges = columnChanges;
        this.indexChanges = indexChanges;
    }

    public String getOriginalName() {
        return originalName;
    }

    public String getCurrentName() {
        return currentName;
    }

    public void setCurrentName(String currentName) {
        this.currentName = currentName;
    }

    public List<TableChange> getColumnChanges() {
        return columnChanges;
    }

    public List<TableChange> getIndexChanges() {
        return indexChanges;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(originalName);
        if (currentName != null)
            str.append("=").append(currentName);
        str.append(columnChanges).append(indexChanges);
        return str.toString();
    }    
}
