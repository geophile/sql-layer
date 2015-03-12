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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.foundationdb.ais.model.validation.AISInvariants;

public class Group extends HasStorage implements Visitable
{
    public static Group create(AkibanInformationSchema ais, String schemaName, String rootTableName)
    {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "Group", "schemaName");
        AISInvariants.checkNullName(rootTableName, "Group", "rootTableName");
        TableName groupName = new TableName(schemaName, rootTableName);
        AISInvariants.checkDuplicateGroups(ais, groupName);
        Group group = new Group(groupName);
        ais.addGroup(group);
        return group;
    }

    private Group(TableName name)
    {
        this.name = name;
        this.indexMap = new HashMap<>();
    }

    public TableName getName()
    {
        return name;
    }

    public String getDescription()
    {
        return name.toString();
    }

    public void setRootTable(Table rootTable)
    {
        this.rootTable = rootTable;
    }

    public Table getRoot()
    {
        return rootTable;
    }

    public Collection<GroupIndex> getIndexes()
    {
        return Collections.unmodifiableCollection(internalGetIndexMap().values());
    }

    public GroupIndex getIndex(String indexName)
    {
        return internalGetIndexMap().get(indexName);
    }

    public void addIndex(GroupIndex index)
    {
        indexMap.put(index.getIndexName().getName(), index);
        GroupIndexHelper.actOnGroupIndexTables(index, GroupIndexHelper.ADD);
    }

    public void removeIndexes(Collection<GroupIndex> indexesToDrop)
    {
        indexMap.values().removeAll(indexesToDrop);
        for (GroupIndex groupIndex : indexesToDrop) {
            GroupIndexHelper.actOnGroupIndexTables(groupIndex, GroupIndexHelper.REMOVE);
        }
    }

    private Map<String, GroupIndex> internalGetIndexMap() {
        return indexMap;
    }

    public boolean isVirtual()
    {
        return (storageDescription != null) && storageDescription.isVirtual();
    }

    // HasStorage

    @Override
    public AkibanInformationSchema getAIS() {
        return rootTable.getAIS();
    }

    @Override
    public String getTypeString() {
        return "Group";
    }

    @Override
    public String getNameString() {
        return name.toString();
    }

    @Override
    public String getSchemaName() {
        return (rootTable != null) ? rootTable.getName().getSchemaName() : null;
    }

    // Visitable

    /** Visit this instance, the root table and then all group indexes. */
    @Override
    public void visit(Visitor visitor) {
        visitor.visit(this);
        rootTable.visit(visitor);
        for(Index i : getIndexes()) {
            i.visit(visitor);
        }
    }

    // State

    private final TableName name;
    private final Map<String, GroupIndex> indexMap;
    private Table rootTable;
}
