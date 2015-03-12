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
package com.foundationdb.server.test.it.tablestatus;

import static org.junit.Assert.assertEquals;

import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.concurrent.Callable;

public class TableStatusRecoveryIT extends ITBase {
    private final static int ROW_COUNT = 100;

    @Test
    public void simpleInsertRowCountTest() throws Exception {
        int tableId = createTable("test", "A", "I INT NOT NULL, V VARCHAR(255), PRIMARY KEY(I)");
        for (int i = 0; i < ROW_COUNT; i++) {
            writeRows(row(tableId, i, "This is record # " + 1));
        }
        assertEquals(ROW_COUNT, getRowCount(tableId));

        safeRestartTestServices();

        assertEquals(ROW_COUNT, getRowCount(tableId));
    }

    @Test
    public void ordinalCreationTest() throws Exception {
        final int aId = createTable("test", "A", "ID INT NOT NULL, PRIMARY KEY(ID)");
        final int aOrdinal = getOrdinal(aId);

        final int bId = createTable("test", "B", "ID INT NOT NULL, AID INT, PRIMARY KEY(ID)", akibanFK("AID", "A", "ID"));
        final int bOrdinal = getOrdinal(bId);
        
        assertEquals("ordinals unique before restart", true, aOrdinal != bOrdinal);

        safeRestartTestServices();

        assertEquals("parent ordinal same after restart", aOrdinal, getOrdinal(aId));
        assertEquals("child ordinal same after restart", bOrdinal, getOrdinal(bId));
        
        final int cId = createTable("test", "C", "ID INT NOT NULL, BID INT, PRIMARY KEY(ID)", akibanFK("BID", "B", "ID"));
        final int cOrdinal = getOrdinal(cId);
        
        assertEquals("new grandchild after restart has unique ordinal", true, cOrdinal != aOrdinal && cOrdinal != bOrdinal);
    }

    private int getOrdinal(final int tableId) throws Exception {
        return getTable(tableId).getOrdinal();
    }

    private long  getRowCount(final int tableId) {
        return txnService().run(session(), new Callable<Long>()
        {
            @Override
            public Long call() throws Exception {
                return getTable(tableId).tableStatus().getRowCount(session());
            }
        });
    }
}
