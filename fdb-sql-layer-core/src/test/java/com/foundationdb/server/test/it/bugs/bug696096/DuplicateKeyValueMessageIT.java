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
package com.foundationdb.server.test.it.bugs.bug696096;

import java.text.MessageFormat;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public final class DuplicateKeyValueMessageIT extends ITBase {
    private int tableId;

    @Before
    public void setUp() throws InvalidOperationException {
        tableId = createTable("sa", "ta",
                "c0 INT NOT NULL PRIMARY KEY",
                "c1 int",
                "c2 int",
                "c3 int",
                "name varchar(32)",
                "UNIQUE (c1, c2)",
                "CONSTRAINT my_key UNIQUE(c3)"
        );
        writeRows(
                row(tableId, 10, 11, 12, 13, "from setup"),
                row(tableId, 20, 21, 22, 23, "from setup")
        );
    }

    @After
    public void tearDown() throws InvalidOperationException {
        expectRows(tableId,
                row(tableId, 10, 11, 12, 13, "from setup"),
                row(tableId, 20, 21, 22, 23, "from setup")
        );
    }

    @Test
    public void writeDuplicatesPrimary() {
        duplicateOnWrite("PRIMARY", 10, 91, 92, 93);
    }

    @Test
    public void writeDuplicatesC1() {
        duplicateOnWrite("c1", 90, 11, 12, 93);
    }

    @Test
    public void writeDuplicatesMyKey() {
        duplicateOnWrite("my_key", 90, 91, 92, 13);
    }

    @Test
    public void writeDuplicatesMultiple() {
        duplicateOnWrite("PRIMARY", 10, 11, 12, 13);
    }

    @Test
    public void updateDuplicatesPrimary() {
        duplicateOnUpdate("PRIMARY", 10, 91, 92, 93);
    }

    @Test
    public void updateDuplicatesC1() {
        duplicateOnUpdate("c1", 90, 11, 12, 93);
    }

    @Test
    public void updateDuplicatesMyKey() {
        duplicateOnUpdate("my_key", 90, 91, 92, 13);
    }

    @Test
    public void updateDuplicatesMultiple() {
        duplicateOnUpdate("PRIMARY", 10, 11, 12, 13);
    }

    private static void dupMessageValid(DuplicateKeyException e, String indexName) {
        final String message = MessageFormat.format(ErrorCode.DUPLICATE_KEY.getMessage(), "sa", "ta", indexName);
        final String expectedMessagePrefix = message.substring(0, message.length()-5);
        
        boolean messageIsValid = e.getShortMessage().startsWith(expectedMessagePrefix);

        if (!messageIsValid) {
            String errString = String.format("expected message to start with <%s>, but was <%s>",
                    expectedMessagePrefix, e.getMessage()
            );
            e.printStackTrace();
            fail(errString);
        }
    }

    private void duplicateOnWrite(String indexName, int c0, int c1, int c2, int c3) {
        try {
            writeRows(row(tableId, c0, c1, c2, c3, "from write"));
        } catch (DuplicateKeyException e) {
            dupMessageValid(e, indexName);
            return;
        } catch (InvalidOperationException e) {
            throw new TestException("unexpected exception", e);
        }
        fail("Excpected DuplicateKeyExcepton");
    }
    
    private void duplicateOnUpdate(String indexName, int c0, int c1, int c2, int c3) {
        try {
            Row oldRow = row(tableId, 20, 21, 22, 23, "from setup");
            Row newRow = row(tableId, c0, c1, c2, c3, "from update");
            updateRow(oldRow, newRow);
        } catch (DuplicateKeyException e) {
            dupMessageValid(e, indexName);
            return;
        } catch (InvalidOperationException e) {
            throw new TestException("unexpected exception", e);
        }
        fail("Excpected DuplicateKeyExcepton");
    }
}
