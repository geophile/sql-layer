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
package com.foundationdb.server.test.it.nopk;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

public final class NoPkIT extends ITBase {
    @Test
    public void replAkiban() throws Exception {
        int tableId = createTable("test", "REPL_C", "I INT");
        writeRows(
                row(tableId, 2),
                row(tableId, 2)
        );

        expectRowsSkipInternal(
                tableId,
                row(tableId, 2),
                row(tableId, 2)
        );
    }

    // Inspired by bug 1023944
    @Test
    public void GIWithPKLessLeafTable_dropLeaf()
    {
        int t1 =
            createTable("schema", "t1",
                        "id int not null",
                        "primary key(id)");
        int t2 =
            createTable("schema", "t2",
                        "id int",
                        "ref int",
                        "grouping foreign key(id) references t1(id)");
        writeRows(
            row(t1, 1),
            row(t1, 2),
            row(t1, 3));
        writeRows( // Bug 1023945 produces NPE when writing rows to t2
                   row(t2, 1, 1),
                   row(t2, 2, 1),
                   row(t2, 3, 1));
        createLeftGroupIndex(new TableName("schema", "t1"), "gi", "t2.ref", "t2.id", "t1.id");
        ddl().dropTable(session(), new TableName("schema", "t2"));
    }

    // Inspired by bug 1023945
    @Test
    public void GIWithPKLessLeafTable_populate()
    {
        int t1 =
            createTable("schema", "t1",
                        "id int not null",
                        "primary key(id)");
        int t2 =
            createTable("schema", "t2",
                        "id int",
                        "ref int",
                        "grouping foreign key(id) references t1(id)");
        createLeftGroupIndex(new TableName("schema", "t1"), "gi", "t2.ref", "t2.id", "t1.id");
        writeRows(
            row(t1, 1),
            row(t1, 2),
            row(t1, 3));
        writeRows( // Bug 1023945 produces NPE when writing rows to t2
            row(t2, 1, 1),
            row(t2, 2, 1),
            row(t2, 3, 1));
    }
}
