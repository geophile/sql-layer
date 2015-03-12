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
package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.delete_Returning;
import static com.foundationdb.qp.operator.API.filter_Default;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertEquals;

public class DeleteIT extends OperatorITBase {
    @Test
    public void deleteCustomer() {
        use(db);
        doDelete();
        compareRows(
                array(Row.class,
                      row(customerRowType, 1L, "xyz")
                ),
                cursor(
                        filter_Default(
                                groupScan_Default(coi),
                                Collections.singleton(customerRowType)),
                        queryContext, queryBindings
                )
        );
    }

    @Test
    public void deleteCustomerCheckNameIndex() {
        use(db);
        doDelete();
        compareRows(
                array(Row.class,
                      row(customerNameIndexRowType, "xyz", 1L)
                      ),
                cursor(
                indexScan_Default(
                        customerNameIndexRowType,
                        IndexKeyRange.unbounded(customerNameIndexRowType),
                        new API.Ordering()),
                queryContext, queryBindings
        ));
    }

    @Test
    public void deleteCustomerCheckNameItemOidGroupIndex() {
        use(db);
        doDelete();
        compareRows(
                array(Row.class,
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 111L),
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 112L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 121L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 122L)
                ),
                cursor(
                indexScan_Default(
                        customerNameItemOidIndexRowType,
                        IndexKeyRange.unbounded(customerNameItemOidIndexRowType),
                        new API.Ordering(),
                        customerRowType),
                queryContext, queryBindings
        ));
    }

    private void doDelete() {
        Row[] rows = {
                row(customerRowType, new Object[]{2, "abc"})
        };
        Operator deletePlan = delete_Returning(rowsToValueScan(rows), false);
        List<Row> result = runPlan(queryContext, queryBindings, deletePlan);
        assertEquals("rows touched", rows.length, result.size());
    }
}
