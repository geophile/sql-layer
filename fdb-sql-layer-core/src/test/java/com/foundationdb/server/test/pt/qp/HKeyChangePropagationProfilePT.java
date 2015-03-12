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
package com.foundationdb.server.test.pt.qp;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.util.tap.Tap;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Callable;

import static com.foundationdb.qp.operator.API.*;

public class HKeyChangePropagationProfilePT extends QPProfilePTBase
{
    @Before
    @Override
    public void setUpProfiling() throws Exception
    {
        // Changes to parent.gid propagate to children hkeys.
        grandparent = createTable(
            "schema", "grandparent",
            "gid int not null primary key",
            "gid_copy int");
        createIndex("schema", "grandparent", "idx_gid_copy", "gid_copy");
        parent = createTable(
            "schema", "parent",
            "pid int not null primary key",
            "gid int",
            "pid_copy int," +
            "grouping foreign key(gid) references grandparent(gid)");
        createIndex("schema", "parent", "idx_pid_copy", "pid_copy");
        child1 = createTable(
            "schema", "child1",
            "cid1 int not null primary key",
            "pid int",
            "cid1_copy int," +
            "grouping foreign key(pid) references parent(pid)");
        createIndex("schema", "child1", "idx_cid1_copy", "cid1_copy");
        child2 = createTable(
            "schema", "child2",
            "cid2 int not null primary key",
            "pid int",
            "cid2_copy int," +
            "grouping foreign key(pid) references parent(pid)");
        createIndex("schema", "child2", "idx_cid2_copy", "cid2_copy");
        schema = SchemaCache.globalSchema(ais());
        grandparentRowType = schema.tableRowType(table(grandparent));
        parentRowType = schema.tableRowType(table(parent));
        child1RowType = schema.tableRowType(table(child1));
        child2RowType = schema.tableRowType(table(child2));
        group = group(grandparent);
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        // The following is adapter from super.setUpProfiling. Leave taps disabled, they'll be enabled after loading
        // and warmup
        beforeProfiling();
        tapsRegexes.clear();
        registerTaps();
    }

    private int        grandparent;
    private int        parent;
    private int child1;
    private int        child2;
    private RowType    grandparentRowType;
    private RowType    parentRowType;
    private RowType    child1RowType;
    private RowType    child2RowType;
    private Group      group;

    protected void populateDB(final int grandparents, 
                              final int parentsPerGrandparent, 
                              final int childrenPerParent) throws Exception
    {
        transactionally(
            new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    long gid = 0;
                    long pid = 0;
                    long cid = 0;
                    for (int c = 0; c < grandparents; c++) {
                        writeRow(grandparent, gid, gid);
                        for (int o = 0; o < parentsPerGrandparent; o++) {
                            writeRow(parent, pid, gid, pid);
                            for (int i = 0; i < childrenPerParent; i++) {
                                writeRow(child1, cid, pid, cid);
                                writeRow(child2, cid, pid, cid);
                                cid++;
                            }
                            pid++;
                        }
                        gid++;
                    }
                    return null;
                }
            });
    }

    @Override
    protected void registerTaps()
    {
        tapsRegexes.add(".*propagate.*");
    }

    @Test
    public void profileHKeyChangePropagationFromParent() throws Exception
    {
        final int WARMUP_SCANS = 10; // Number of times to update each parent.gid during warmup
        final int SCANS = 100; // Number of times to update each parent.gid
        final int GRANDPARENTS = 1;
        final int PARENTS_PER_GRANDPARENT = 10;
        final int CHILDREN_PER_PARENT = 100;
        populateDB(GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT);
        Operator scanPlan =
            filter_Default(
                groupScan_Default(group),
                Collections.singleton(parentRowType));
        final Operator updatePlan = update_Returning(scanPlan,
                           new UpdateFunction()
                           {
                               @Override
                               public Row evaluate(Row original, QueryContext context, QueryBindings bindings)
                               {
                                   OverlayingRow updatedRow = new OverlayingRow(original);
                                   long i = original.value(1).getInt64();
                                   updatedRow.overlay(1, i - 1000000);
                                   return updatedRow;
                               }
                           });
        long start = Long.MIN_VALUE;
        for (int s = 0; s < WARMUP_SCANS + SCANS; s++) {
            final int sFinal = s;
            long mightBeStartTime = transactionally(
                new Callable<Long>()
                {
                    @Override
                    public Long call() throws Exception
                    {
                        long start = -1L;
                        if (sFinal == WARMUP_SCANS) {
                            Tap.setEnabled(".*propagate.*", true);
                            start = System.nanoTime();
                        }
                        runPlan(queryContext, queryBindings, updatePlan);
                        return start;
                    }
                });
            if (mightBeStartTime != -1L) {
                start = mightBeStartTime;
            }
        }
        long end = System.nanoTime();
        assert start != Long.MIN_VALUE;
        double sec = (end - start) / (1000.0 * 1000 * 1000);
        System.out.println(String.format("scans: %s, db: %s/%s/%s, time: %s",
                                         SCANS, GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT, sec));
    }

    @Test
    public void profileHKeyChangePropagationFromGrandparent() throws Exception
    {
        final int WARMUP_SCANS = 10; // Number of times to update each parent.gid during warmup
        final int SCANS = 100; // Number of times to update each parent.gid
        final int GRANDPARENTS = 1;
        final int PARENTS_PER_GRANDPARENT = 10;
        final int CHILDREN_PER_PARENT = 100;
        populateDB(GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT);
        Operator scanPlan =
            limit_Default(
                filter_Default(
                    groupScan_Default(group),
                    Collections.singleton(grandparentRowType)),
                1);
        final Operator updatePlan = update_Returning(scanPlan,
                           new UpdateFunction()
                           {
                               @Override
                               public Row evaluate(Row original, QueryContext context, QueryBindings bindings)
                               {
                                   OverlayingRow updatedRow = new OverlayingRow(original);
                                   long i = original.value(0).getInt64();
                                   updatedRow.overlay(0, i - 1000000);
                                   return updatedRow;
                               }
                           });
        final Operator revertPlan = update_Returning(scanPlan,
                           new UpdateFunction()
                           {
                               @Override
                               public Row evaluate(Row original, QueryContext context, QueryBindings bindings)
                               {
                                   OverlayingRow updatedRow = new OverlayingRow(original);
                                   long i = original.value(0).getInt64();
                                   updatedRow.overlay(0, i + 1000000);
                                   return updatedRow;
                               }
                           });
        long start = Long.MIN_VALUE;
        for (int s = 0; s < WARMUP_SCANS + SCANS; s++) {
            final int sFinal = s;
            long mightBeStartTime = transactionally(
                new Callable<Long>()
                {
                    @Override
                    public Long call() throws Exception
                    {
                        long start = -1L;
                        if (sFinal == WARMUP_SCANS) {
                            Tap.setEnabled(".*propagate.*", true);
                            start = System.nanoTime();
                        }
                        runPlan(queryContext, queryBindings, updatePlan);
                        runPlan(queryContext, queryBindings, revertPlan);
                        return start;
                    }
                });
            if (mightBeStartTime != -1L) {
                start = mightBeStartTime;
            }
        }
        long end = System.nanoTime();
        assert start != Long.MIN_VALUE;
        double sec = (end - start) / (1000.0 * 1000 * 1000);
        System.out.println(String.format("scans: %s, db: %s/%s/%s, time: %s",
                                         SCANS, GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT, sec));
    }
}
