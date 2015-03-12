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
package com.foundationdb.server.test.it.bugs.bug701580;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public final class SpuriousDuplicateKeyIT extends ITBase {
    @Test
    public void simpleOnce() throws Exception {
        simpleTestCase();
    }

    @Test
    public void simpleTwice() throws Exception {
        simpleTestCase();
        simpleTestCase();
    }

    private void simpleTestCase() throws Exception {
        createTable("test", "t1", "bid1 int not null, token varchar(64), primary key(bid1)");
        createIndex("test", "t1", "token", "token");
        int t2 = createTable("test", "t2", "bid int not null, theme varchar(64), primary key (bid), unique(theme)");

        confirmIds("t1", 1, 2);
        confirmIds("t2", 1, 2);

        writeRows(
                row(t2, 1, "0"),
                row(t2, 2, "1"),
                row(t2, 3, "2")
        );
        dropAllTables();
    }

    @Test
    public void indexIdsLocalToGroup() throws Exception {
        createTable("test", "t1", "bid1 int not null, token varchar(64), primary key(bid1)");
        createIndex("test", "t1", "token", "token");

        createTable("test", "t2", "bid int not null, theme varchar(64), primary key (bid), unique (theme)");
        createTable("test", "t3", "id int not null primary key, bid_id int, "+
                    "GROUPING FOREIGN KEY (bid_id) REFERENCES t2 (bid)");
        createIndex("test", "t3", "__akiban_fk", "bid_id");

        confirmIds("t1", 1, 2);
        confirmIds("t2", 1, 2);
        confirmIds("t3", 3, 2);
    }

    /**
     * Confirm that the given table has sequential index IDs starting from the given number, and that its
     * group table has all those indexes as well.
     * @param tableName the table to start at
     * @param startingAt the index to start at
     * @param expectedUIndexes how many indexes you expect on the user table
     * @throws Exception if there's a problem!
     */
    private void confirmIds(String tableName, int startingAt, int expectedUIndexes)
            throws Exception {
        Table table = ddl().getAIS(session()).getTable("test", tableName);

        Set<Integer> expectedUTableIds = new HashSet<>();
        Set<Integer> actualUTableIds = new HashSet<>();
        for (Index index : table.getIndexes()) {
            actualUTableIds.add(index.getIndexId());
            expectedUTableIds.add( expectedUTableIds.size() + startingAt );
        }

        assertEquals("table index count", expectedUIndexes, actualUTableIds.size());
    }
}
