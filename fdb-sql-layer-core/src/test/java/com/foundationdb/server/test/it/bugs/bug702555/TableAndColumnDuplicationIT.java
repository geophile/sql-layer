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
package com.foundationdb.server.test.it.bugs.bug702555;

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

public final class TableAndColumnDuplicationIT extends ITBase {

    @Test
    public void sameTableAndColumn() throws InvalidOperationException {
        doTest( "tbl1", "id1",
                "tbl1", "id1");
    }

    @Test
    public void sameTableDifferentColumn() throws InvalidOperationException {
        doTest( "tbl1", "id1",
                "tbl1", "id2");
    }

    @Test
    public void differentTableSameColumn() throws InvalidOperationException {
        doTest( "tbl1", "id1",
                "tbl2", "id1");
    }

    @Test
    public void differentTableDifferentColumn() throws InvalidOperationException {
        doTest( "tbl1", "id1",
                "tbl2", "id2");
    }

    /**
     * A potentially more subtle problem. No duplicate key exceptions are thrown, because the two tables have
     * inherently incompatible primary keys. But data gets written to the same tree, and thus a read stumbles
     * across rows it shouldn't see
     * @throws InvalidOperationException if any CRUD operation fails
     */
    @Test
    public void noDuplicateKeyButIncompatibleRows() throws InvalidOperationException {
        final int schema1Table
                = createTable("schema1", "table1", "id int not null primary key");
        final int schema2Table =
                createTable("schema2","table1", "name varchar(32) not null primary key");

        writeRows(
                row(schema1Table, 0),
                row(schema1Table, 1),
                row(schema1Table, 2)
        );

        writeRows(
                row(schema2Table, "first row"),
                row(schema2Table, "second row"),
                row(schema2Table, "third row")
        );

        expectRows(schema1Table,
                row(schema1Table, 0),
                row(schema1Table, 1),
                row(schema1Table, 2)
        );
        
        expectRows(schema2Table,
                row(schema2Table, "first row"),
                row(schema2Table, "second row"),
                row(schema2Table, "third row")
        );
    }

    private void doTest(String schema1TableName, String schema1TableKeyCol,
                        String schema2TableName, String schema2TableKeyCol) throws InvalidOperationException
    {
        final int schema1Table
                = createTable("schema1", schema1TableName, schema1TableKeyCol + " int not null primary key, name varchar(32)");
        final int schema2Table =
                createTable("schema2", schema2TableName, schema2TableKeyCol + " int not null primary key, name varchar(32)");

        writeRows(
                row(schema1Table, 0, "alpha-0"),
                row(schema1Table, 1, "alpha-1"),
                row(schema1Table, 2, "alpha-1")
        );

        writeRows(
                row(schema2Table, 0, "bravo-0"),
                row(schema2Table, 1, "bravo-1"),
                row(schema2Table, 2, "bravo-1")
        );

        expectRows( schema1Table,
                row(schema1Table, 0, "alpha-0"),
                row(schema1Table, 1, "alpha-1"),
                row(schema1Table, 2, "alpha-1")
        );

        expectRows( schema2Table,
                row(schema2Table, 0, "bravo-0"),
                row(schema2Table, 1, "bravo-1"),
                row(schema2Table, 2, "bravo-1")
        );
    }
}
