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

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilder;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadHelper.UncaughtHandler;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConcurrentUniqueInsertMT extends MTBase
{
    private int tid;
    private RowType rowType;

    private void createNotNullUnique() {
        tid = createTable("test", "t", "id INT NOT NULL PRIMARY KEY, x INT NOT NULL, UNIQUE(x)");
        rowType = SchemaCache.globalSchema(ais()).tableRowType(tid);
    }

    private void createNullUnique() {
        tid = createTable("test", "t", "id INT NOT NULL PRIMARY KEY, x INT NULL, UNIQUE(x)");
        rowType = SchemaCache.globalSchema(ais()).tableRowType(tid);
    }

    private List<MonitoredThread> buildThreads(Integer x1, Integer x2) {
        ConcurrentTestBuilder builder = ConcurrentTestBuilderImpl.create();
        builder.add("insert_a", insertCreator(tid, testRow(rowType, 1, x1)))
               .sync("sync", Stage.POST_BEGIN)
               .add("insert_b", insertCreator(tid, testRow(rowType, 2, x2)))
               .sync("sync", Stage.POST_BEGIN);
        return builder.build(this);
    }

    private void expectSuccess(List<MonitoredThread> threads) {
        ThreadHelper.runAndCheck(threads);
    }

    private void expectOneFailure(List<MonitoredThread> threads) {
        UncaughtHandler handler = ThreadHelper.startAndJoin(threads);
        assertEquals("failure count", 1, handler.thrown.size());
        Throwable failure = handler.thrown.values().iterator().next();
        assertEquals("failure was dup", DuplicateKeyException.class, failure.getClass());
        assertEquals("saw rollback", true, threads.get(0).hadRollback() || threads.get(1).hadRollback());
    }

    @Test
    public void notNullSameValue() {
        createNotNullUnique();
        expectOneFailure(buildThreads(42, 42));
    }

    @Test
    public void notNullDifferentValue() {
        createNotNullUnique();
        expectSuccess(buildThreads(42, 52));
    }

    @Test
    public void nullBothNull() {
        createNullUnique();
        expectSuccess(buildThreads(null, null));
    }

    @Test
    public void nullSameValue() {
        createNotNullUnique();
        expectOneFailure(buildThreads(42, 42));
    }

    @Test
    public void nullDifferentValue() {
        createNotNullUnique();
        expectSuccess(buildThreads(42, 52));
    }
}
