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

import com.foundationdb.ais.model.validation.AISInvariants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ForeignKey implements Constraint
{
    public static enum Action {
        NO_ACTION, RESTRICT, CASCADE, SET_NULL, SET_DEFAULT;

        public String toSQL() {
            return name().replace('_', ' ');
        }
    }

    public static ForeignKey create(AkibanInformationSchema ais,
                                    String constraintName,
                                    Table referencingTable,
                                    List<Column> referencingColumns,
                                    Table referencedTable,
                                    List<Column> referencedColumns,
                                    Action deleteAction,
                                    Action updateAction,
                                    boolean deferrable,
                                    boolean initiallyDeferred) {
        ais.checkMutability();
        ForeignKey fk = new ForeignKey(constraintName,
                                       referencingTable, referencingColumns,
                                       referencedTable, referencedColumns,
                                       deleteAction, updateAction,
                                       deferrable, initiallyDeferred);
        AISInvariants.checkDuplicateConstraintsInSchema(ais, fk.getConstraintName());
        referencingTable.addForeignKey(fk);
        referencedTable.addForeignKey(fk);
        if (constraintName != null) {
            ais.addConstraint(fk);
        }
        return fk;
    }

    /** Find an index on {@code table} starting with
     * {@code requiredColumns} in some order. */
    public static TableIndex findIndex(Table table,
                                       List<Column> requiredColumns,
                                       boolean requireUnique) {
        int ncols = requiredColumns.size();
        for (TableIndex index : table.getIndexesIncludingInternal()) {
            if (requireUnique) {
                if (!index.isUnique() || (index.getKeyColumns().size() != ncols)) {
                    continue;
                }
            } else if (index.getKeyColumns().size() < ncols) {
                continue;
            }
            boolean found = true;
            for (int i = 0; i < ncols; i++) {
                if (requiredColumns.indexOf(index.getKeyColumns().get(i).getColumn()) < 0) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return index;
            }
        }
        return null;
    }

    /** Find a unique index on <code>referencedTable</code> exactly
     * <code>referencedColumns</code> in some order.
     */
    public static TableIndex findReferencedIndex(Table referencedTable,
                                                 List<Column> referencedColumns) {
        return findIndex(referencedTable, referencedColumns, true);
    }

    /** Find an index on {@code referencingTable} starting with
     * {@code referencingColumns} in some order. */
    public static TableIndex findReferencingIndex(Table referencingTable,
                                                  List<Column> referencingColumns) {
        return findIndex(referencingTable, referencingColumns, false);
    }

    @Override
    public Table getConstraintTable() {
        return getReferencingTable();
    }

    public TableName getConstraintName() {
        return constraintName;
    }

    public Table getReferencingTable() {
        return join.getChild();
    }

    public List<Column> getReferencingColumns() {
        return join.getChildColumns();
    }

    public TableIndex getReferencingIndex() {
        if (referencingIndex == null) {
            synchronized (this) {
                if (referencingIndex == null) {
                    referencingIndex = findReferencingIndex(join.getChild(), join.getChildColumns());
                }
            }
        }
        return referencingIndex;
    }

    public Table getReferencedTable() {
        return join.getParent();
    }

    public List<Column> getReferencedColumns() {
        return join.getParentColumns();
    }

    public TableIndex getReferencedIndex() {
        if (referencedIndex == null) {
            synchronized (this) {
                if (referencedIndex == null) {
                    referencedIndex = findReferencedIndex(join.getParent(), join.getParentColumns());
                }
            }
        }
        return referencedIndex;
    }

    public List<JoinColumn> getJoinColumns() {
        return join.getJoinColumns();
    }
    
    public Action getDeleteAction() {
        return deleteAction;
    }

    public Action getUpdateAction() {
        return updateAction;
    }
    
    public boolean isDeferrable() {
        return deferrable;
    }

    public boolean isInitiallyDeferred() {
        return initiallyDeferred;
    }

    public boolean isDeferred(Map<ForeignKey,Boolean> transactionDeferred) {
        if (!deferrable)
            return false;
        if (transactionDeferred != null) {
            Boolean result = transactionDeferred.get(null);
            if (result != null)
                return result;
            result = transactionDeferred.get(this);
            if (result != null)
                return result;
        }
        return initiallyDeferred;
    }

    public static Map<ForeignKey,Boolean> setDeferred(Map<ForeignKey,Boolean> transactionDeferred,
                                                      ForeignKey fkey, boolean deferred) {
        if (transactionDeferred == null)
            transactionDeferred = new HashMap<>();
        if (fkey == null) {
            transactionDeferred.clear();
        } else if(!fkey.isDeferrable()) {
            throw new IllegalArgumentException("Not deferrable: " + fkey);
        }
        transactionDeferred.put(fkey, deferred);
        return transactionDeferred;
    }

    @Override
    public String toString()
    {
        return "Foreign Key " + constraintName.getTableName() + ": " + join.getChild() + " REFERENCES " + join.getParent(); 
    }

    private ForeignKey(String constraintName,
                       Table referencingTable,
                       List<Column> referencingColumns,
                       Table referencedTable,
                       List<Column> referencedColumns,
                       Action deleteAction,
                       Action updateAction,
                       boolean deferrable,
                       boolean initiallyDeferred) {
        this.constraintName = new TableName(referencingTable.getName().getSchemaName(), constraintName);
        this.deleteAction = deleteAction;
        this.updateAction = updateAction;
        this.deferrable = deferrable;
        this.initiallyDeferred = initiallyDeferred;
        
        join = Join.create(constraintName, referencedTable, referencingTable);
        for (int i = 0; i < referencingColumns.size(); i++) {
            join.addJoinColumn(referencedColumns.get(i), referencingColumns.get(i));
        }
        
    }

    // NOTE: referencingColumns (join#childColumns) and 
    // referencedColumns (join#parentColumns)  are in
    // declaration order and parallel to one another. They are not
    // necessarily in the order of referencingIndex or
    // referencedIndex.

    private final TableName constraintName;
    private final Action deleteAction;
    private final Action updateAction;
    private final boolean deferrable, initiallyDeferred;
    private volatile TableIndex referencingIndex;
    private volatile TableIndex referencedIndex;
    private Join join;

}
