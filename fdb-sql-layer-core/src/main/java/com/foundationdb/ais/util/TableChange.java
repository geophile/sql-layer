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
package com.foundationdb.ais.util;

public class TableChange {
    public static enum ChangeType { ADD, DROP, MODIFY }

    private final String oldName;
    private final String newName;
    private final ChangeType changeType;

    private TableChange(String oldName, String newName, ChangeType changeType) {
        this.oldName = oldName;
        this.newName = newName;
        this.changeType = changeType;
    }

    public String getOldName() {
        return oldName;
    }

    public String getNewName() {
        return newName;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    @Override
    public String toString() {
        if(oldName != null && newName == null)
            return changeType + ":" + oldName;
        if(oldName == null && newName != null)
            return changeType + ":" + newName;
        return changeType + ":" + oldName + "->" + newName;
    }

    public static TableChange createAdd(String name) {
        return new TableChange(null, name, ChangeType.ADD);
    }

    public static TableChange createDrop(String name) {
        return new TableChange(name, null, ChangeType.DROP);
    }

    public static TableChange createModify(String oldName, String newName) {
        return new TableChange(oldName, newName, ChangeType.MODIFY);
    }
}
