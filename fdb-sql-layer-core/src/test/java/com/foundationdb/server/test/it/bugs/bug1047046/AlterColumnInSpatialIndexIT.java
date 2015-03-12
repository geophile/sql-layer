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
package com.foundationdb.server.test.it.bugs.bug1047046;

import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.test.it.dxl.AlterTableITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AlterColumnInSpatialIndexIT extends AlterTableITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t1";
    private static final String INDEX_NAME = "idx1";
    private static final int ROW_COUNT = 3;

    public void createAndLoadTable() {
        int tid = createTable(SCHEMA, TABLE, "c1 decimal(11,7), c2 decimal(11,7)");
        writeRows(
                row(tid, "43.5435", "156.989"),
                row(tid, "32.456", "99.543"),
                row(tid, "53.00", "80.00")
        );
        createIndex(SCHEMA, TABLE, INDEX_NAME, "geo_lat_lon(c1, c2)");
        TableIndex index = getTable(tid).getIndex("idx1");
        assertNotNull("Found index", index);
        assertEquals("Is spatial", true, index.isSpatial());
    }

    // From bug report
    @Test
    public void alterToIncompatible() {
        createAndLoadTable();
        runAlter("ALTER TABLE t1 ALTER c2 SET DATA TYPE varchar(10)");
        final int tid = tableId(SCHEMA, TABLE);
        assertEquals("row count", ROW_COUNT, scanAll(tid).size());
        assertEquals("Index exists", false, getTable(tid).getIndex(INDEX_NAME) != null);
    }

    @Test
    public void alterToCompatible() {
        createAndLoadTable();
        final int tid = tableId(SCHEMA, TABLE);
        TableIndex indexBefore = getTable(tid).getIndex(INDEX_NAME);
        assertEquals("index row count before alter", ROW_COUNT, scanAllIndex(indexBefore).size());
        runAlter("ALTER TABLE t1 ALTER c2 SET DATA TYPE decimal(22,14)");
        assertEquals("row count", ROW_COUNT, scanAll(tid).size());
        TableIndex index = getTable(tid).getIndex(INDEX_NAME);
        assertEquals("Index exists", true, index != null);
        assertEquals("index row count", ROW_COUNT, scanAllIndex(index).size());
    }

    @Test
    public void dropColumn2() {
        createAndLoadTable();
        runAlter("ALTER TABLE t1 DROP COLUMN c2");
        final int tid = tableId(SCHEMA, TABLE);
        assertEquals("row count", ROW_COUNT, scanAll(tid).size());
        assertEquals("Index exists", false, getTable(tid).getIndex(INDEX_NAME) != null);
    }
    
    @Test
    public void dropColumn1() {
        createAndLoadTable();
        runAlter("ALTER TABLE t1 DROP COLUMN c1");
        final int tid = tableId(SCHEMA, TABLE);
        assertEquals("row count", ROW_COUNT, scanAll(tid).size());
        assertEquals("Index exists", false, getTable(tid).getIndex(INDEX_NAME) != null);
    }
}
