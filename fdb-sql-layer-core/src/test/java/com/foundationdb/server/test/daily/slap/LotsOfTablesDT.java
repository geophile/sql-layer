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
package com.foundationdb.server.test.daily.slap;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.test.daily.DailyBase;

/**
 * This test simply creates and then drops 2000 tables. Prior to the
 * fix for 772047 this caused an OOME.  Completion of this test constitutes
 * success.
 * @author peter
 *
 */
public class LotsOfTablesDT extends DailyBase {
    private final static int TABLE_COUNT = 2000;

    @Test
    public void createLotsOfTablesTest() throws Exception {
        int was = -1;
        for (int count = 0; count < TABLE_COUNT; count++) {
            String tableName = String.format("test%04d", count);
            int tableId = createTable("test", tableName,
                    "I INT NOT NULL, V VARCHAR(255), PRIMARY KEY(I)");
            assertTrue(was != tableId);
            was = tableId;
        }

        for (int count = 0; count < TABLE_COUNT; count++) {
            String tableName = String.format("test%04d", count);
            ddl().dropTable(session(), new TableName("test", tableName));
        }
    }
}
