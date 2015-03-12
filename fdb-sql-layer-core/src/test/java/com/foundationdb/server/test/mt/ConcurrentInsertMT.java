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

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilder;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import org.junit.Test;

import java.util.List;

import static com.foundationdb.qp.operator.API.insert_Returning;
import static com.foundationdb.qp.operator.API.valuesScan_Default;

public class ConcurrentInsertMT extends MTBase
{
    public static int ROW_COUNT = 100;
    private final int TIMEOUT = 10 * 1000;

    private RowType rowType;

    private void createHiddenPK() {
        int tid = createTable("test", "t", "id INT");
        rowType = SchemaCache.globalSchema(ais()).tableRowType(tid);
    }

    private void createExplicitPK() {
        int tid = createTable("test", "t", "id INT NOT NULL PRIMARY KEY, x INT");
        rowType = SchemaCache.globalSchema(ais()).tableRowType(tid);
    }

    private void run(int threadCount) {
        ConcurrentTestBuilder builder = ConcurrentTestBuilderImpl.create();
        int perThread = ROW_COUNT / threadCount;
        for(int i = 0; i < threadCount; ++i) {
            builder.add("thread_"+i, insertCreator(rowType, 1 + i * perThread, perThread))
                   .sync("a", Stage.PRE_BEGIN);
        }
        List<MonitoredThread> threads = builder.build(this);
        ThreadHelper.runAndCheck(TIMEOUT,threads);
    }

    private static OperatorCreator insertCreator(final RowType rowType, int startID, int count) {
        final Row[] rows = new Row[count];
        for(int i = 0; i < count; ++i) {
            rows[i] = new TestRow(rowType, startID + i, startID + i);
        }
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType outType = schema.tableRowType(rowType.typeId());
                return insert_Returning(valuesScan_Default(bindableRows(rows), outType));
            }
        };
    }


    @Test
    public void hiddenPKTwoThreads() {
        createHiddenPK();
        run(2);
    }

    @Test
    public void hiddenPKTenThreads() {
        createHiddenPK();
        run(10);
    }

    @Test
    public void explicitPKTwoThreads() {
        createExplicitPK();
        run(2);
    }

    @Test
    public void explicitPKTenThreads() {
        createExplicitPK();
        run(10);
    }
}
