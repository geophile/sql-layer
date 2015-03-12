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
package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class BasicKeyUpdateIT extends ITBase {
    protected static final String SCHEMA = "cold";
    protected static final String TABLE = "frosty";

    @Test
    public void oldKeysAreRemoved_1Row() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "c", 2)
                ),

                row(tableId, 2, "c"),
                row(tableId, 2, "a"),

                Arrays.asList(
                        row(tableId, 2, "a")
                ),
                Arrays.asList(
                        row(index, "a", 2)
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_Partial_IndexChanged() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "b", 1),
                        row(index, "c", 2)
                ),

                row(tableId, 2, "c"),
                row(tableId, 2, "a"),

                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "a")
                ),
                Arrays.asList(
                        row(index, "a", 2),
                        row(index, "b", 1)
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_IndexChanged() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "b", 1),
                        row(index, "c", 2)
                ),

                row(tableId, 2, "c"),
                row(tableId, 2, "a"),

                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "a")
                ),
                Arrays.asList(
                        row(index, "a", 2),
                        row(index, "b", 1)
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_IndexAndPKMovesBackward() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "b", 1),
                        row(index, "c", 2)
                ),

                row(tableId, 2, "c"),
                row(tableId, 0, "a"),

                Arrays.asList(
                        row(tableId, 0, "a"),
                        row(tableId, 1, "b")
                ),
                Arrays.asList(
                        row(index, "a", 0),
                        row(index, "b", 1)
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_IndexAndPKMovesForward() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "b", 1),
                        row(index, "c", 2)
                ),

                row(tableId, 2, "c"),
                row(tableId, 3, "a"),

                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 3, "a")
                ),
                Arrays.asList(
                        row(index, "a", 3),
                        row(index, "b", 1)
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_IndexSame() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "d"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "c", 2),
                        row(index, "d", 1)
                ),

                row(tableId, 2, "c"),
                row(tableId, 2, "a"),

                Arrays.asList(
                        row(tableId, 1, "d"),
                        row(tableId, 2, "a")
                ),
                Arrays.asList(
                        row(index, "a", 2),
                        row(index, "d", 1)
                )
        );
    }

    public void runTest(int tableId,
                        List<Row> initialRows,
                        List<Row> initialRowsByName,
                        Row rowToUpdate,
                        Row updatedValue,
                        List<Row> endRows,
                        List<Row> endRowsByName

    ) throws InvalidOperationException
    {
        Set<Integer> sizes = new HashSet<>();
        sizes.add( initialRows.size() );
        sizes.add( initialRowsByName.size() );
        sizes.add( endRows.size() );
        sizes.add( endRowsByName.size() );
        if(sizes.size() != 1) {
            throw new RuntimeException("All lists must be of the same size");
        }

        writeRows( initialRows.toArray(new Row[initialRows.size()]) );
        expectRows(
            nameIndex(),
            initialRowsByName
        );

        updateRow(rowToUpdate, updatedValue);

        expectRows(
                tableId,
                endRows.toArray(new Row[endRows.size()])
        );

        expectRows(
            nameIndex(),
            endRowsByName
        );
    }

    private int table() throws InvalidOperationException {
        int tid = createTable(SCHEMA, TABLE, "id int not null primary key", "name varchar(32)");
        createIndex(SCHEMA, TABLE, "name", "name");
        return tid;
    }

    private Index nameIndex() {
        return getTable(SCHEMA, TABLE).getIndex("name");
    }
}
