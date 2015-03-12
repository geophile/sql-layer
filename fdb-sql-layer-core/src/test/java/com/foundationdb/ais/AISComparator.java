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
package com.foundationdb.ais;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;

import java.util.Collection;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class AISComparator {
    public static void compareAndAssert(AkibanInformationSchema lhs, AkibanInformationSchema rhs, boolean withIDs) {
        compareAndAssert("", lhs, rhs, withIDs);
    }
    
    public static void compareAndAssert(String msgPrefix, AkibanInformationSchema lhs, AkibanInformationSchema rhs, boolean withIDs) {
        String realPrefix = msgPrefix.length() > 0 ? msgPrefix + ": " : "";
        
        assertEquals(realPrefix + "AIS charsets",
                     lhs.getCharsetName(), rhs.getCharsetName());
        assertEquals(realPrefix + "AIS collations",
                     lhs.getCollationName(), rhs.getCollationName());

        GroupMaps lhsGroups = new GroupMaps(lhs.getGroups().values(), withIDs);
        GroupMaps rhsGroups = new GroupMaps(rhs.getGroups().values(), withIDs);
        lhsGroups.compareAndAssert(realPrefix, rhsGroups);

        TableMaps lhsTables = new TableMaps(lhs.getTables().values(), withIDs);
        TableMaps rhsTables = new TableMaps(rhs.getTables().values(), withIDs);
        lhsTables.compareAndAssert(realPrefix, rhsTables);
    }

    private static class GroupMaps {
        public final Collection<TableName> names = new TreeSet<>();
        public final Collection<String> indexes = new TreeSet<>();

        public GroupMaps(Collection<Group> groups, boolean withIDs) {
            for(Group group : groups) {
                names.add(group.getName());
                for(Index index : group.getIndexes()) {
                    indexes.add(index.toString() + (withIDs ? index.getIndexId() : ""));
                }
            }
        }

        public void compareAndAssert(String msgPrefix, GroupMaps rhs) {
            assertEquals(msgPrefix + "Group names", names.toString(), rhs.names.toString());
            assertEquals(msgPrefix + "Group indexes", indexes.toString(), rhs.indexes.toString());
        }
    }

    private static class TableMaps {
        public final Collection<String> names = new TreeSet<>();
        public final Collection<String> indexes = new TreeSet<>();
        public final Collection<String> columns = new TreeSet<>();
        public final Collection<String> charAndCols = new TreeSet<>();

        public TableMaps(Collection<Table> tables, boolean withIDs) {
            for(Table table : tables) {
                names.add(table.getName().toString() + (withIDs ? table.getTableId() : ""));
                for(Column column : table.getColumnsIncludingInternal()) {
                    columns.add(column.toString() + " " + column.getTypeDescription() + " " + column.getCharsetName() + "/" + column.getCollationName());
                }
                for(Index index : table.getIndexesIncludingInternal()) {
                    indexes.add(index.toString() + (withIDs ? index.getIndexId() : ""));
                }
                charAndCols.add(table.getName() + " " + table.getDefaultedCharsetName() + "/" + table.getDefaultedCollationName());
            }
        }

        public void compareAndAssert(String msgPrefix, TableMaps rhs) {
            assertEquals(msgPrefix + "Table names", names.toString(), rhs.names.toString());
            assertEquals(msgPrefix + "Table columns", columns.toString(), rhs.columns.toString());
            assertEquals(msgPrefix + "Table indexes", indexes.toString(), rhs.indexes.toString());
            assertEquals(msgPrefix + "Table charAndCols", charAndCols.toString(), rhs.charAndCols.toString());
        }
    }
}
