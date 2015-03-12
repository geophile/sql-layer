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
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.ExpressionGenerators;

import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.qp.operator.API.*;

public class ProjectCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB(ROWS);
        run(WARMUP_RUNS, false);
        run(MEASURED_RUNS, true);
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String tableName = newTableName();
        t = createTable(schemaName, tableName,
                        "id int not null",
                        "primary key(id)");
        schema = SchemaCache.globalSchema(ais());
        tRowType = schema.tableRowType(table(t));
        group = group(t);
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB(int rows)
    {
        for (int id = 0; id < rows; id++) {
            writeRow(t, id);
        }
    }

    private void run(int runs, boolean report)
    {
        Operator scan = groupScan_Default(group);
        TimeOperator timeScan = new TimeOperator(scan);
        Operator project = project_DefaultTest(timeScan, tRowType, Arrays.asList(ExpressionGenerators.literal(true)));
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(project, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long projectNsec = stop - start - timeScan.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = projectNsec / (1000.0 * runs * ROWS);
            System.out.println(String.format("%s usec/row", averageUsecPerRow));
        }
    }

    private static final int ROWS = 100;
    private static final int WARMUP_RUNS = 20000;
    private static final int MEASURED_RUNS = 10000;

    private int t;
    private RowType tRowType;
    private Group group;
}
