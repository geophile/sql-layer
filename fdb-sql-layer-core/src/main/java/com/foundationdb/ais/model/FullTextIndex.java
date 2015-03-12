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
import com.foundationdb.server.error.BranchingGroupIndexException;
import com.foundationdb.server.error.IndexColNotInGroupException;

import java.util.*;

public class FullTextIndex extends Index
{
    /* Index */

    @Override
    public HKey hKey() {
        return indexedTable.hKey();
    }

    @Override
    public boolean isTableIndex() {
        return false;
    }

    @Override
    public boolean isGroupIndex() {
        return false;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.FULL_TEXT;
    }

    @Override
    public IndexMethod getIndexMethod() {
        return IndexMethod.FULL_TEXT;
    }

    @Override
    public void computeFieldAssociations(Map<Table,Integer> ordinalMap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table leafMostTable() {
        // This is not entirely well-defined, since more than one
        // descendant to the same depth can be indexed.
        Table deepest = null;
        for (IndexColumn indexColumn : keyColumns) {
            if ((deepest == null) || 
                (indexColumn.getColumn().getTable().getDepth() > deepest.getDepth())) {
                deepest = indexColumn.getColumn().getTable();
            }
        }
        return deepest;
    }

    @Override
    public Table rootMostTable() {
        Table shallowest = null;
        for (IndexColumn indexColumn : keyColumns) {
            if ((shallowest == null) || 
                (indexColumn.getColumn().getTable().getDepth() < shallowest.getDepth())) {
                shallowest = indexColumn.getColumn().getTable();
            }
        }
        return shallowest;
    }

    @Override
    public void checkMutability() {
        indexedTable.checkMutability();
    }

    @Override
    public Collection<Integer> getAllTableIDs() {
        Set<Integer> ids = new HashSet<>();
        for (IndexColumn indexColumn : keyColumns) {
            ids.add(indexColumn.getColumn().getTable().getTableId());
        }
        ids.add(indexedTable.getTableId());
        return ids;
    }

    @Override
    public void addColumn(IndexColumn indexColumn) {
        Table table = indexColumn.getColumn().getTable();
        if (!((table == indexedTable) ||
              table.isDescendantOf(indexedTable) ||
              indexedTable.isDescendantOf(table))) {
            if (table.getGroup() != indexedTable.getGroup()) {
                throw new IndexColNotInGroupException(indexColumn.getIndex().getIndexName().getName(),
                                                      indexColumn.getColumn().getName());
            }
            else {
                throw new BranchingGroupIndexException(indexColumn.getIndex().getIndexName().getName(),
                                                       table.getName(),
                                                       indexedTable.getName());
            }
        }
        super.addColumn(indexColumn);
        table.addFullTextIndex(this);
    }

    /* FullTextIndex */

    public Table getIndexedTable() {
        return indexedTable;
    }

    public static FullTextIndex create(AkibanInformationSchema ais,
                                       Table table, String indexName,
                                       Integer indexId) {
        return create(ais, table, indexName, indexId, null);
    }

    public static FullTextIndex create(AkibanInformationSchema ais,
                                       Table table, String indexName, 
                                       Integer indexId, TableName constraintName)
    {
        ais.checkMutability();
        if(constraintName != null) {
            throw new IllegalArgumentException("Full Text indexes are never constraints");
        }
        table.checkMutability();
        AISInvariants.checkDuplicateIndexesInTable(table, indexName);
        FullTextIndex index = new FullTextIndex(table, indexName, indexId);
        table.addFullTextIndex(index);
        return index;
    }

    private FullTextIndex(Table indexedTable, String indexName, Integer indexId)
    {
        super(indexedTable.getName(), indexName, indexId, false, false, null, null);
        this.indexedTable = indexedTable;
    }
    
    private final Table indexedTable;
}
