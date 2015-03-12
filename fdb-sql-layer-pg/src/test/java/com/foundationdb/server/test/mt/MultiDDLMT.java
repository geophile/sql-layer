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
package com.foundationdb.server.test.mt;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/** Multiple threads executing non-overlapping DDL. */
@RunWith(NamedParameterizedRunner.class)
public class MultiDDLMT extends PostgresMTBase
{
    private static final int LOOPS = 10;

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        return Arrays.asList(Parameterization.create("oneThread", 1),
                             Parameterization.create("twoThreads", 2),
                             Parameterization.create("fiveThreads", 5));
    }

    private static final class DDLWorker extends QueryThread
    {
        public DDLWorker(String schema, Connection conn) {
            super("DDL_"+schema, schema, conn);
        }

        @Override
        protected int getLoopCount() {
            return LOOPS;
        }

        @Override
        protected String[] getQueries() {
            String table = String.format("\"%s\".t", schema);
            return new String[] {
                "CREATE TABLE "+table+"(id INT NOT NULL PRIMARY KEY, x INT)",
                "INSERT INTO "+table+" VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
                "CREATE INDEX x ON "+table+"(x)",
                "DROP TABLE "+table
            };
        }
    }

    private final int threadCount;

    public MultiDDLMT(int threadCount) {
        this.threadCount = threadCount;
    }

    @Test
    public void runTest() throws Exception {
        List<DDLWorker> threads = new ArrayList<>(threadCount);
        for(int i = 0; i < threadCount; ++i) {
            threads.add(new DDLWorker("test_" + i, createConnection()));
        }
        ThreadHelper.runAndCheck(60000, threads);
    }
}
