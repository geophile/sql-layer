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

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.util.ArgumentValidation;

import java.util.Collection;
import java.util.Map;

/**
 * Information describing the state of an altered table
 */
public class ChangedTableDescription {
    public static enum ParentChange {
        /** No change at all **/
        NONE,
        /** Metadata only change (e.g. rename o.cid) **/
        META,
        /** Group is changing but not the relationship (e.g. i is UPDATE when c.id changes type) **/
        UPDATE,
        /** New parent **/
        ADD,
        /** Dropped parent **/
        DROP
    }

    private final int tableID;
    private final TableName tableName;
    private final Table newDefinition;
    private final Map<String,String> colNames;
    private final ParentChange parentChange;
    private final TableName parentName;
    private final Map<String,String> parentColNames;
    private final Map<String,String> preserveIndexes;
    private final Collection<TableName> droppedSequences;
    private final Collection<String> identityAdded;
    private final Collection<String> indexesAdded;
    private final boolean isTableAffected;
    private final boolean isPKAffected;

    /**
     * @param tableName Current name of the table being changed.
     * @param newDefinition New definition of the table.
     * @param preserveIndexes Mapping of new index names to old.
     */
    public ChangedTableDescription(int tableID, TableName tableName, Table newDefinition, Map<String,String> colNames,
                                   ParentChange parentChange, TableName parentName, Map<String,String> parentColNames,
                                   Map<String, String> preserveIndexes, Collection<TableName> droppedSequences,
                                   Collection<String> identityAdded, Collection<String> indexesAdded,
                                   boolean isTableAffected, boolean isPKAffected) {
        ArgumentValidation.notNull("tableName", tableName);
        ArgumentValidation.notNull("preserveIndexes", preserveIndexes);
        this.tableID = tableID;
        this.tableName = tableName;
        this.newDefinition = newDefinition;
        this.colNames = colNames;
        this.parentChange = parentChange;
        this.parentName = parentName;
        this.parentColNames = parentColNames;
        this.preserveIndexes = preserveIndexes;
        this.droppedSequences = droppedSequences;
        this.identityAdded = identityAdded;
        this.indexesAdded = indexesAdded;
        this.isTableAffected = isTableAffected;
        this.isPKAffected = isPKAffected;
    }

    public int getTableID() {
        return tableID;
    }

    public TableName getOldName() {
        return tableName;
    }

    public TableName getNewName() {
        return (newDefinition != null) ? newDefinition.getName() : tableName;
    }

    public Table getNewDefinition() {
        return newDefinition;
    }

    public Map<String,String> getColNames() {
        return colNames;
    }

    public ParentChange getParentChange() {
        return parentChange;
    }

    public TableName getParentName() {
        return parentName;
    }

    public Map<String,String> getParentColNames() {
        return parentColNames;
    }

    public Map<String,String> getPreserveIndexes() {
        return preserveIndexes;
    }

    public Collection<TableName> getDroppedSequences() {
        return droppedSequences;
    }

    public Collection<String> getIdentityAdded() {
        return identityAdded;
    }

    public Collection<String> getIndexesAdded() {
        return indexesAdded;
    }

    public boolean isTableAffected() {
        return isTableAffected;
    }

    public boolean isPKAffected() {
        return isPKAffected;
    }

    public boolean isNewGroup() {
        return (parentChange != ParentChange.NONE) && (parentChange != ParentChange.META);
    }

    @Override
    public String toString() {
        return toString(getOldName(), getNewName(), isNewGroup(), getParentChange(), getPreserveIndexes());
    }

    public static String toString(TableName oldName, TableName newName, boolean newGroup, ParentChange groupChange, Map<String,String> preservedIndexMap) {
        return oldName + "=" + newName + "[newGroup=" + newGroup + "][parentChange=" + groupChange + "]" + preservedIndexMap.toString();
    }
}
