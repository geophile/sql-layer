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
package com.foundationdb.server.test.it.bugs.bug1145249;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

public class ManyLongTextFieldsIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";

    private void runTest(int fieldCount) {
        TestAISBuilder builder = new TestAISBuilder(typesRegistry());
        builder.table(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "id", 0, "MCOMPAT", "int", false);
        builder.pk(SCHEMA, TABLE);
        builder.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "id", 0, true, null);
        for(int i = 1; i <= fieldCount; ++i) {
            String colName = "lt_" + i;
            builder.column(SCHEMA, TABLE, colName, i, "MCOMPAT", "LONGTEXT", true);
        }
        Table table = builder.akibanInformationSchema().getTable(SCHEMA, TABLE);
        ddl().createTable(session(), table);

        Object[] colValues = new Object[fieldCount + 1];
        colValues[0] = 1;
        for(int i = 1; i <= fieldCount; ++i) {
            colValues[i] = "longtext_value";
        }

        int tid = tableId(SCHEMA, TABLE);
        Row row = row(tid, colValues);
        writeRow(row);
        expectRows(tid, row);
    }

    @Test
    public void oneField() {
        runTest(1);
    }

    @Test
    public void fiveFields() {
        runTest(5);
    }

    @Test
    public void tenFields() {
        runTest(10);
    }

    @Test
    public void twentyFields() {
        runTest(20);
    }

    @Test
    public void fiftyFields() {
        runTest(50);
    }
}
