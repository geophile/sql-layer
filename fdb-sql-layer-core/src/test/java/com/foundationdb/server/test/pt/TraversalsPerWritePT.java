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
package com.foundationdb.server.test.pt;

import com.foundationdb.ais.model.Index;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.qp.row.Row;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

/**
 * <p>Test to profile traverse counts for various CUD operations. The test is on a modified COI: there is no
 * items table, and customers has additional children tables only in some parameterizations. There are three
 * parameterizations to this test:
 * <ol>
 *     <li>how many children rows are in each of the customer's child tables (1 or 20)</li>
 *     <li>whether the GI is a LEFT or RIGHT join</li>
 *     <li>whether the schema is bushy or "narrow"</li>
 * </ol>
 * </p>
 *
 * <p>If the schema is narrow, customers only has one child table: orders. If it's bushy, the customers table has
 * three additional children: addresses, pets and vehicles. The orders table is created after addresses, so that
 * the ordinals are (a, o, p, v).</p>
 *
 * <p>To set up, we create two customers, at cid(1) and cid(9). We then create children rows for cid(0) (these rows
 * are all orphaned) and cid(1). cid(9) has no children.</p>
 *
 * <p>Each test then writes, deletes or moves (updates) various rows.</p>
 */
@RunWith(NamedParameterizedRunner.class)
public final class TraversalsPerWritePT extends PTBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        pb.add("");
        pb.multiplyParametersByAppending("1", 1, "20", 20);
        pb.multiplyParametersByAppending("-LEFT", Index.JoinType.LEFT, "-RIGHT", Index.JoinType.RIGHT);
        pb.multiplyParametersByAppending("-narrow", false, "-bushy", true);
        return pb.asList();
    }

    @Override
    protected void registerTaps() {
        tapsRegexes.add("travers.*");
    }

    @Test
    public void deleteCustomer1() {
        deleteRow(cTable, 1L, "alpha");
    }

    @Test
    public void writeCustomer0() {
        writeRow(cTable, 0L, "adopter");
    }

    @Test
    public void moveCustomer1To2() {
        updateRow(row(cTable, 1L, "alpha"), row(cTable, 2L, "beta"));
    }

    @Test
    public void moveCustomer1To0() {
        updateRow(row(cTable, 1L, "alpha"), row(cTable, 0L, "adopter"));
    }

    @Test
    public void writeOrderForCustomer0() {
        writeRows(ordersRow(0, ordersPerCustomer + 1));
    }

    @Test
    public void writeOrderForCustomer1() {
        writeRows(ordersRow(1, ordersPerCustomer+1));
    }

    @Test
    public void writeOrderForCustomer9() {
        writeRows(ordersRow(9, ordersPerCustomer+1));
    }

    @Test
    public void deleteOrderForCustomer0() {
        deleteRow(ordersRow(0, ordersPerCustomer));
    }

    @Test
    public void deleteOrderForCustomer1() {
        deleteRow(ordersRow(1, ordersPerCustomer));
    }

    @Test
    public void moveOrderForCustomer0ToCustomer0() {
        updateRow(ordersRow(0, ordersPerCustomer), ordersRow(0, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer0ToCustomer1() {
        updateRow(ordersRow(0, ordersPerCustomer), ordersRow(1, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer0ToCustomer9() {
        updateRow(ordersRow(0, ordersPerCustomer), ordersRow(9, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer1ToCustomer0() {
        updateRow(ordersRow(1, ordersPerCustomer), ordersRow(0, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer1ToCustomer1() {
        updateRow(ordersRow(1, ordersPerCustomer), ordersRow(1, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer1ToCustomer9() {
        updateRow(ordersRow(1, ordersPerCustomer), ordersRow(9, ordersPerCustomer+10));
    }

    @Override
    protected String paramName() {
        return String.format("%d-%s-%s", ordersPerCustomer, joinType, bushy ? "bushy" : "narrow");
    }

    @Override
    protected void beforeProfiling() {
        cTable = createTable(SCHEMA, "customers", "cid int key, name varchar(32)");
        int aTable = createTable(SCHEMA, "addresses", "aid int key, cid int, where varchar(32)",
                akibanFK("cid", "customers", "cid")
        );
        // creating the oTable after aTable will give it a higher ordinal, sandwiching it in the bushy group
        oTable = createTable(SCHEMA, "orders", "oid int key, cid int, when varchar(32)",
                akibanFK("cid", "customers", "cid)"));
        int pTable = createTable(SCHEMA, "pets", "pid int key, cid int, where varchar(32)",
                akibanFK("cid", "customers", "cid")
        );
        int vTable = createTable(SCHEMA, "vehicles", "vid int key, cid int, model varchar(32)",
                akibanFK("cid", "customers", "cid")
        );

        // write one customer
        writeRow(cTable, 1L, "alpha");
        // write orders for two customers, one of which (cid=0) doesn't exist
        for (long cid = 0; cid < 2; ++cid) {
            for (long oidSegment = 1; oidSegment <= ordersPerCustomer; ++oidSegment) {
                Row row = ordersRow(cid, oidSegment);
                writeRows(row);
                if (bushy) {
                    writeRows(
                            customersChild(aTable,  cid, oidSegment),
                            customersChild(pTable,  cid, oidSegment),
                            customersChild(vTable,  cid, oidSegment)
                    );
                }
            }
        }
        // write a third customer
        writeRow(cTable, 9L, "joda"); // like "iota", the ninth letter. This joke is a bit "forced"

        createGroupIndex(
                getTable(cTable).getGroup().getName(),
                "test_gi",
                joinType,
                "customers.name", "orders.when"
        );
    }

    private Row ordersRow(long cid, long oidSegment) {
        return customersChild(oTable, cid, oidSegment);
    }

    private Row customersChild(int tableId, long cid, long oidSegment) {
        long oid = cid + oidSegment  * 10;
        return row(tableId, oid, cid, String.valueOf(1900 + oid));
    }

    public TraversalsPerWritePT(int ordersPerCustomer, Index.JoinType joinType, boolean bushy) {
        this.ordersPerCustomer = ordersPerCustomer;
        this.joinType = joinType;
        this.bushy = bushy;
    }

    private final int ordersPerCustomer;
    private final Index.JoinType joinType;
    private final boolean bushy;

    private int oTable;
    private int cTable;

    private static final String SCHEMA = "tpwpt";
}
