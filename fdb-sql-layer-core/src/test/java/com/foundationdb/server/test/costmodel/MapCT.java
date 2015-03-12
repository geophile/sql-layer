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
package com.foundationdb.server.test.costmodel;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.InvalidOperationException;

import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;

public class MapCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        run(WARMUP_RUNS, 5, false);
        for (int innerRows : INNER_ROWS_PER_OUTER) {
            run(MEASURED_RUNS, innerRows, true);
        }
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String pTableName = newTableName();
        p = createTable(schemaName, pTableName,
                        "pid int not null",
                        "primary key(pid)");
        String cTableName = newTableName();
        c = createTable(schemaName, cTableName,
                        "cid int not null",
                        "pid int",
                        "primary key(cid)",
                        String.format("grouping foreign key(pid) references %s(pid)", pTableName));
        String dTableName = newTableName();
        d = createTable(schemaName, dTableName,
                        "did int not null",
                        "pid int",
                        "primary key(did)",
                        String.format("grouping foreign key(pid) references %s(pid)", pTableName));
        schema = SchemaCache.globalSchema(ais());
        pRowType = schema.tableRowType(table(p));
        cRowType = schema.tableRowType(table(c));
        dRowType = schema.tableRowType(table(d));
        group = group(p);
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB()
    {
        for (int r = 0; r < OUTER_ROWS; r++) {
            writeRow(p, r);
        }
    }
    
    private void run(int runs, int innerRows, boolean report)
    {
        Operator setupOuter = groupScan_Default(group);
        TimeOperator timeSetupOuter = new TimeOperator(setupOuter);
        Operator setupInner = limit_Default(groupScan_Default(group), innerRows);
        TimeOperator timeSetupInner = new TimeOperator(setupInner);
        Operator plan = map_NestedLoops(timeSetupOuter, timeSetupInner, 
                                        0, pipelineMap(), 1);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long mapNsec = stop - start - timeSetupInner.elapsedNsec() - timeSetupOuter.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = mapNsec / (1000.0 * runs * (OUTER_ROWS * (innerRows + 1)));
            System.out.println(String.format("inner/outer = %s: %s usec/row",
                                             innerRows, averageUsecPerRow));
        }
    }

    private static final int WARMUP_RUNS = 1000;
    private static final int MEASURED_RUNS = 1000;
    private static final int OUTER_ROWS = 100;
    private static final int[] INNER_ROWS_PER_OUTER = new int[]{64, 32, 16, 8, 4, 2, 1, 0};

    private int p;
    private int c;
    private int d;
    private TableRowType pRowType;
    private TableRowType cRowType;
    private TableRowType dRowType;
    private Group group;
}
