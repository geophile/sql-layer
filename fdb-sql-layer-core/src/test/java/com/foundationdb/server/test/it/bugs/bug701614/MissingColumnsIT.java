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
package com.foundationdb.server.test.it.bugs.bug701614;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.util.Strings;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public final class MissingColumnsIT extends ITBase {
    @Test
    public void testForMissingColumns() throws InvalidOperationException, IOException {
        int tableId = loadBlocksTable();
        writeRows( rows(tableId) );
        expectRows(tableId, rows(tableId));
    }

    private int loadBlocksTable() throws InvalidOperationException, IOException {
        final String blocksDDL = Strings.join(Strings.dumpResource(getClass(), "blocks-table.ddl"));
        createFromDDL("drupal", blocksDDL);
        AkibanInformationSchema ais = ddl().getAIS(session());
        assertNotNull("drupal.blocks missing from " + ais.getTables(), ais.getTable("drupal", "blocks"));
        return tableId("drupal", "blocks");
    }

    private Row[] rows(int tableId) {
        return new Row[] {
                row(tableId, 1, "user", "0", "garland", 1, 0, "left", 0, 0, 0, "", "", -1),
                row(tableId, 2, "user", "1", "garland", 1, 0, "left", 0, 0, 0, "", "", -1),
                row(tableId, 3, "system", "0", "garland", 1, 10, "footer", 0, 0, 0, "", "", -1)
        };
    }
}
