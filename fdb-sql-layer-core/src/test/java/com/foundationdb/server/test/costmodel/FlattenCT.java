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
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.error.InvalidOperationException;

import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;

public class FlattenCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        for (JoinType joinType : JoinType.values()) {
            run(WARMUP_RUNS, 4, joinType, false);
            for (int childCount : CHILD_COUNTS) {
                run(MEASURED_RUNS, childCount, joinType, true);
            }
        }
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String parentTableName = newTableName();
        parent = createTable(schemaName, parentTableName,
                        "pid int not null",
                        "parent_instance int not null",
                        "primary key(pid, parent_instance)");
        String childTableName = newTableName();
        child = createTable(schemaName, childTableName,
                            "cid int not null",
                            "pid int",
                            "parent_instance int",
                            "primary key(cid)",
                            String.format("grouping foreign key(pid, parent_instance) references %s(pid, parent_instance)", 
                                          parentTableName));
        schema = SchemaCache.globalSchema(ais());
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        parentPKIndexType = indexType(parent, "pid", "parent_instance");
        group = group(parent);
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB()
    {
        int cid = 0;
        for (int childCount : CHILD_COUNTS) {
            for (int i = 0; i < PARENT_INSTANCES; i++) {
                writeRow(parent, childCount, i);
                for (int c = 0; c < childCount; c++) {
                    writeRow(child, cid++, childCount, i);
                }
            }
        }
    }
    
    private void run(int runs, int childCount, JoinType joinType, boolean report)
    {
        IndexBound pid = new IndexBound(row(parentPKIndexType, childCount), new SetColumnSelector(0));
        IndexKeyRange pidRange = IndexKeyRange.bounded(parentPKIndexType, pid, true, pid, true);
        Operator setup =
            branchLookup_Default(
                indexScan_Default(parentPKIndexType, false, pidRange),
                group,
                parentPKIndexType,
                parentRowType,
                InputPreservationOption.DISCARD_INPUT);
        TimeOperator timeSetup = new TimeOperator(setup);
        Operator plan =
            flatten_HKeyOrdered(
                timeSetup,
                parentRowType,
                childRowType,
                joinType);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long flattenNsec = stop - start - timeSetup.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = flattenNsec / (1000.0 * runs * (childCount + 1));
            System.out.println(String.format("%s childCount = %s: %s usec/row",
                                             joinType, childCount, averageUsecPerRow));
        }
    }

    // The database has parents with varying numbers of children. For each such number, there are PARENT_INSTANCES
    // parent rows.
    private static final int PARENT_INSTANCES = 100;
    private static final int WARMUP_RUNS = 10000;
    private static final int MEASURED_RUNS = 1000;
    private static final int[] CHILD_COUNTS = new int[]{64, 32, 16, 8, 4, 2, 1, 0};

    private int parent;
    private int child;
    private TableRowType parentRowType;
    private TableRowType childRowType;
    private IndexRowType parentPKIndexType;
    private Group group;
}
