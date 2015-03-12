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

import java.util.ArrayList;

public class AISTableNameChanger {
    public AISTableNameChanger(Table table)
    {
        this.table = table;
        this.newSchemaName = table.getName().getSchemaName();
        this.newTableName = table.getName().getTableName();
    }

    public AISTableNameChanger(Table table, TableName newName) {
        this(table, newName.getSchemaName(), newName.getTableName());
    }

    public AISTableNameChanger(Table table, String newSchemaName, String newTableName) {
        this.table = table;
        this.newSchemaName = newSchemaName;
        this.newTableName = newTableName;
    }

    public void setSchemaName(String newSchemaName) {
        this.newSchemaName = newSchemaName;
    }

    public void setNewTableName(String newTableName) {
        this.newTableName = newTableName;
    }

    public void doChange() {
        AkibanInformationSchema ais = table.getAIS();
        ais.removeTable(table.getName());
        TableName newName = new TableName(newSchemaName, newTableName);

        // Fix indexes because index names incorporate table name
        for (Index index : table.getIndexesIncludingInternal()) {
            index.setIndexName(new IndexName(newName, index.getIndexName().getName()));
        }
        // Join names too. Copy the joins because ais.getJoins() will be updated inside the loop
        NameGenerator nameGenerator = new DefaultNameGenerator();
        for (Join join : new ArrayList<>(ais.getJoins().values())) {
            if (join.getParent().getName().equals(table.getName())) {
                String newJoinName = nameGenerator.generateJoinName(newName,
                                                                    join.getChild().getName(),
                                                                    join.getJoinColumns());
                join.replaceName(newJoinName);
            } else if (join.getChild().getName().equals(table.getName())) {
                String newJoinName = nameGenerator.generateJoinName(join.getParent().getName(),
                                                                    newName,
                                                                    join.getJoinColumns());
                join.replaceName(newJoinName);
            }
        }
        // Rename the table and put back in AIS
        table.setTableName(newName);
        ais.addTable(table);
    }


    Table table;
    String newSchemaName;
    String newTableName;
}
