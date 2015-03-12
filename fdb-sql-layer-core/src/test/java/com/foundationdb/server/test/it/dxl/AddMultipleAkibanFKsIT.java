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
package com.foundationdb.server.test.it.dxl;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// Inspired by bug 874459. These DDL steps simulate MySQL running ALTER TABLE statements which add Akiban FKs.

public class AddMultipleAkibanFKsIT extends ITBase
{
    @Test
    public void createRenameCreate()
    {
        createTable("schema", "root", "id int not null, primary key(id)");
        // Create children
        createTable("schema", "child1", "id int not null, rid int, primary key(id)");
        createTable("schema", "child2", "id int not null, rid int, primary key(id)");
        createTable("schema", "child3", "id int not null, rid int, primary key(id)");
        // Add Akiban FK to child1
        createTable("schema", "TEMP", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        ddl().renameTable(session(), tableName("schema", "child1"), tableName("schema", "TEMP2"));
        ddl().renameTable(session(), tableName("schema", "TEMP"), tableName("schema", "child1"));
        ddl().dropTable(session(), tableName("schema", "TEMP2"));
        // Add Akiban FK to child2
        createTable("schema", "TEMP", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        ddl().renameTable(session(), tableName("schema", "child2"), tableName("schema", "TEMP2"));
        ddl().renameTable(session(), tableName("schema", "TEMP"), tableName("schema", "child2"));
        ddl().dropTable(session(), tableName("schema", "TEMP2"));
        // Add Akiban FK to child3
        createTable("schema", "TEMP", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        ddl().renameTable(session(), tableName("schema", "child3"), tableName("schema", "TEMP2"));
        ddl().renameTable(session(), tableName("schema", "TEMP"), tableName("schema", "child3"));
        ddl().dropTable(session(), tableName("schema", "TEMP2"));
        AkibanInformationSchema ais = ddl().getAIS(session());
        Table root = ais.getTable("schema", "root");
        int check = 0;
        for (Join join : root.getChildJoins()) {
            Table child = join.getChild();
            assertEquals(root, join.getParent());
            assertEquals(join, child.getParentJoin());
            String childName = child.getName().getTableName();
            if (childName.equals("child1")) {
                check |= 0x1;
            } else if (childName.equals("child2")) {
                check |= 0x2;
            } else if (childName.equals("child3")) {
                check |= 0x4;
            } else {
                fail();
            }
        }
        assertEquals(0x7, check);
        for (Table table : ais.getTables().values()) {
            assertTrue(!table.getName().getTableName().startsWith("TEMP"));
        }
    }
}
