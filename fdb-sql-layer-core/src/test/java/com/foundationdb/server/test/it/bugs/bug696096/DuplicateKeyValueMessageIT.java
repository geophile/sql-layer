/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
