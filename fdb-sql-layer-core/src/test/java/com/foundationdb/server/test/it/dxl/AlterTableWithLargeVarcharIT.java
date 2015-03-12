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
package com.foundationdb.server.test.it.dxl;

import com.foundationdb.server.test.it.ITBase;
import com.google.common.base.Strings;
import org.junit.Test;

public final class AlterTableWithLargeVarcharIT extends ITBase {
    @Test
    public void reallocation() {
        int tableId = createTable("myschema", "mytable", "id INT NOT NULL PRIMARY KEY",
                "vc_col0 VARCHAR(500), vc_col1 VARCHAR(500)");
        String bigString = Strings.repeat("a", 476);
        writeRow(tableId, 1L, bigString, "hi");
        // Originally triggered a bug in sizing of the null map portion of the RowData
        for (int i = 0; i < 6; ++i) {
            runAlter("myschema", null, "alter table mytable add intcol_" + i + " INTEGER");
        }
    }
}
