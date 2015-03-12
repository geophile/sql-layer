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
package com.foundationdb.qp.rowtype;

import com.foundationdb.ais.model.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

import java.util.*;

/** Table RowTypes are indexed by the Table's ID. Derived RowTypes get higher values. */
public class Schema
{
    public static Set<RowType> descendentTypes(RowType ancestorType, Set<? extends RowType> allTypes)
    {
        Set<RowType> descendentTypes = new HashSet<>();
        for (RowType type : allTypes) {
            if (type != ancestorType && ancestorType.ancestorOf(type)) {
                descendentTypes.add(type);
            }
        }
        return descendentTypes;
    }

    public AggregatedRowType newAggregateType(RowType parent, int inputsIndex, List<? extends TInstance> pAggrTypes)
    {
        return new AggregatedRowType(this, nextTypeId(), parent, inputsIndex, pAggrTypes);
    }

    public FlattenedRowType newFlattenType(RowType parent, RowType child)
    {
        return new FlattenedRowType(this, nextTypeId(), parent, child);
    }

    public ProjectedRowType newProjectType(List<? extends TPreparedExpression> tExprs)
    {
        return new ProjectedRowType(this, nextTypeId(), tExprs);
    }

    public ProductRowType newProductType(RowType leftType, TableRowType branchType, RowType rightType)
    {
        return new ProductRowType(this, nextTypeId(), leftType, branchType, rightType);
    }

    public ValuesRowType newValuesType(TInstance... fields)
    {
        return new ValuesRowType(this, nextTypeId(), fields);
    }

    public HKeyRowType newHKeyRowType(HKey hKey)
    {
        return hKeyRowTypes.get(hKey.table().getTableId());
    }

    public BufferRowType bufferRowType(RowType rightType)
    {
        ValuesRowType leftType = newValuesType(InternalIndexTypes.LONG.instance(false));
        return new BufferRowType(this, nextTypeId(), leftType, rightType);
    }

    public TableRowType tableRowType(Table table)
    {
        return tableRowType(table.getTableId());
    }

    public TableRowType tableRowType(int tableID)
    {
        return (TableRowType) rowTypes.get(tableID);
    }

    public IndexRowType indexRowType(Index index)
    {
        return
            index.isTableIndex()
            ? tableRowType(index.leafMostTable()).indexRowType(index)
            : groupIndexRowType((GroupIndex) index);
    }

    public Set<TableRowType> userTableTypes()
    {
        Set<TableRowType> userTableTypes = new HashSet<>();
        for (AisRowType rowType : rowTypes.values()) {
            if (rowType instanceof TableRowType) {
                if (!rowType.table().isAISTable()) {
                    userTableTypes.add((TableRowType) rowType);
                }
            }
        }
        return userTableTypes;
    }
    public Set<RowType> allTableTypes()
    {
        Set<RowType> allTableTypes = new HashSet<>();
        for (RowType rowType : rowTypes.values()) {
            if (rowType instanceof TableRowType) {
                allTableTypes.add(rowType);
            }
        }
        return allTableTypes;
    }

    public List<IndexRowType> groupIndexRowTypes() {
        return groupIndexRowTypes;
    }

    public Schema(AkibanInformationSchema ais)
    {
        this.ais = ais;
        // Create RowTypes for AIS Tables
        for (Table table : ais.getTables().values()) {
            TableRowType tableRowType = new TableRowType(this, table);
            int tableTypeId = tableRowType.typeId();
            rowTypes.put(tableTypeId, tableRowType);
            typeIdToLeast(tableRowType.typeId());
            
            HKeyRowType hKeyRowType = new HKeyRowType (this, nextTypeId(), table.hKey());
            hKeyRowTypes.put(tableTypeId, hKeyRowType);
        }
        // Create RowTypes for AIS TableIndexes
        for (Table table : ais.getTables().values()) {
            TableRowType tableRowType = tableRowType(table);
            for (TableIndex index : table.getIndexesIncludingInternal()) {
                IndexRowType indexRowType = IndexRowType.createIndexRowType(this, nextTypeId(), tableRowType, index);
                tableRowType.addIndexRowType(indexRowType);
                rowTypes.put(indexRowType.typeId(), indexRowType);
            }
        }
        // Create RowTypes for AIS GroupIndexes
        for (Group group : ais.getGroups().values()) {
            for (GroupIndex groupIndex : group.getIndexes()) {
                IndexRowType indexRowType =
                    IndexRowType.createIndexRowType(this, nextTypeId(), tableRowType(groupIndex.leafMostTable()), groupIndex);
                rowTypes.put(indexRowType.typeId(), indexRowType);
                groupIndexRowTypes.add(indexRowType);
            }
        }
    }

    public AkibanInformationSchema ais()
    {
        return ais;
    }

    // For use by this package

    // For use by this class

    private IndexRowType groupIndexRowType(GroupIndex groupIndex)
    {
        for (IndexRowType groupIndexRowType : groupIndexRowTypes) {
            if (groupIndexRowType.index() == groupIndex) {
                return groupIndexRowType;
            }
        }
        return null;
    }


    private synchronized int nextTypeId()
    {
        return ++typeIdCounter;
    }

    private synchronized void typeIdToLeast(int minValue) {
        typeIdCounter = Math.max(typeIdCounter, minValue);
    }

    // Object state

    private int typeIdCounter = -1;
    private final AkibanInformationSchema ais;
    private final Map<Integer, AisRowType> rowTypes = new HashMap<>();
    private final Map<Integer, HKeyRowType> hKeyRowTypes = new HashMap<>();
    private final List<IndexRowType> groupIndexRowTypes = new ArrayList<>();
}
