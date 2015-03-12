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

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.sql.TestBase;
import org.junit.runner.RunWith;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

@RunWith(NamedParameterizedRunner.class)
public class PostgresServerBinaryTrueIT extends PostgresServerBinaryITBase
{
    public PostgresServerBinaryTrueIT(String caseName, String sql, String expected, String error, String[] params) {
        super(caseName, sql, expected, error, params);
        if ("types".equals(caseName)) {
            try {
                // Because of how java.sql.Timestamp is formatted.
                this.expected = TestBase.fileContents(new File(RESOURCE_DIR, "types.bexpected"));
            }
            catch (IOException ex) {
                fail(ex.getMessage());
            }
        }
    }

    @Override
    protected boolean isBinaryTransfer() {
        return true;
    }
}
