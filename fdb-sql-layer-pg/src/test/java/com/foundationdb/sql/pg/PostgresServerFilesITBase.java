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
package com.foundationdb.sql.pg;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.TestBase;

import com.google.common.io.ByteStreams;
import org.junit.Ignore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * A base class for integration tests that use data from files to specify the
 * input and output expected from calls to the Postgres server.
 */
@Ignore
public class PostgresServerFilesITBase extends PostgresServerITBase
{
    public void loadDatabase(File dir) throws Exception {
        this.rootTableId = super.loadDatabase(SCHEMA_NAME, dir);
    }

    protected int rootTableId;

    protected String dumpData() throws Exception {
        final StringBuilder str = new StringBuilder();
        for(Row row : scanAll(getTable(rootTableId).getGroup())) {
            str.append(row.rowType().table().getName().getTableName());
            for (int i = 0; i < row.rowType().nFields(); i++) {
                str.append(",");
                str.append(ValueSources.toObject(row.value(i)));
            }
            str.append("\n");
        }
        return str.toString();
    }

    protected String caseName, sql, expected, error;
    protected String[] params;

    /** Parameterized version. */
    protected PostgresServerFilesITBase(String caseName, String sql, 
					String expected, String error,
					String[] params) {
        this.caseName = caseName;
        this.sql = sql.trim();
        this.expected = expected;
        this.error = error;
        this.params = params;
    }

    protected PostgresServerFilesITBase() {
    }

    protected void generateAndCheckResult() throws Exception {
        TestBase.generateAndCheckResult((TestBase.GenerateAndCheckResult)this, 
                                        caseName, expected, error);
    }

    /** Copy a resource from the test jar into a temp file so that it
     * can be read by things that do not take a URL.
     */
    protected File copyResourceToTempFile(String resource) throws IOException {
        File tempFile = File.createTempFile(getClass().getSimpleName(), null);
        tempFile.deleteOnExit();
        try (InputStream istr = getClass().getResourceAsStream(resource);
             OutputStream ostr = new FileOutputStream(tempFile)) {
            ByteStreams.copy(istr, ostr);
        }
        return tempFile;
    }

}
