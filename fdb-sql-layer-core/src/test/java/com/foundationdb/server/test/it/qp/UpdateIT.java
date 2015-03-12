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
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedFunction;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSources;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.foundationdb.qp.operator.API.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class UpdateIT extends OperatorITBase
{
    @Test
    public void basicUpdate() throws Exception {
        use(db);

        UpdateFunction updateFunction = new UpdateFunction() {
            @Override
            public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
                String name = ValueSources.toStringSimple(original.value(1));
                name = name.toUpperCase();
                name = name + name;
                return new OverlayingRow(original).overlay(1, name);
            }
        };

        Operator groupScan = groupScan_Default(coi);
        Operator filterScan = filter_Default(groupScan, Collections.singleton(customerRowType));
        Operator updateOperator = update_Returning(filterScan, updateFunction);
        List<Row> result = runPlan(queryContext, queryBindings, updateOperator);
        assertEquals("rows modified", 2, result.size());

        Cursor executable = cursor(groupScan, queryContext, queryBindings);
        Row[] expected = new Row[]{row(customerRowType, 1L, "XYZXYZ"),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(itemRowType, 112L, 11L),
                                   row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(itemRowType, 122L, 12L),
                                   row(customerRowType, 2L, "ABCABC"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L)
        };
        compareRows(expected, executable);
    }
    
    @Test
    public void changePrimaryKeys() throws Exception {
        use(db);

        Operator scan = filter_Default(
            ancestorLookup_Default(
                indexScan_Default(itemIidIndexRowType),
                coi,
                itemIidIndexRowType,
                Arrays.asList(itemRowType),
                InputPreservationOption.DISCARD_INPUT),
            Arrays.asList(itemRowType));
        
        UpdateFunction updateFunction = new UpdateFunction() {
                @Override
                public Row evaluate(Row original, QueryContext context, QueryBindings bindings) { 
                    long id = original.value(0).getInt32();
                    // Make smaller to avoid Halloween (see next test).
                    return new OverlayingRow(original).overlay(0,
                            new Value(original.rowType().typeAt(0), (int)(id - 100)));
                }
            };

        Operator updateOperator = update_Returning(scan, updateFunction);
        List<Row> result = runPlan(queryContext, queryBindings, updateOperator);
        assertEquals("rows touched", 8, result.size());

        Cursor executable = cursor(scan, queryContext, queryBindings);
        Row[] expected = new Row[] { 
            row(itemRowType, 11L, 11L),
            row(itemRowType, 12L, 11L),
            row(itemRowType, 21L, 12L),
            row(itemRowType, 22L, 12L),
            row(itemRowType, 111L, 21L),
            row(itemRowType, 112L, 21L),
            row(itemRowType, 121L, 22L),
            row(itemRowType, 122L, 22L),
        };
        compareRows(expected, executable);
    }

    /**
     * http://en.wikipedia.org/wiki/Halloween_Problem
     *
     * <p>
     * Test the UPDATE of a PRIMARY column driven by a scan of the PRIMARY index itself.
     * </p>
     *
     * For example,
     * <pre>
     * Update_Returning(id+1000)
     *   Filter(item)
     *     AncestorLookup()
     *       IndexScan(item.PRIMARY)
     * </pre>
     *
     * Will get transformed during optimization to:
     * <pre>
     * Insert_Returning()
     *   Project(id+1000)
     *     Buffer()
     *       Delete_Returning()
     *         Filter(item)
     *           AncestorLookup(item)
     *             IndexScan(item.PRIMARY)
     * </pre>
     */
    @Test
    public void halloweenProblem() throws Exception {
        use(db);

        // Basic scan
        final Operator pkScan = filter_Default(
            ancestorLookup_Default(
                indexScan_Default(itemIidIndexRowType),
                coi,
                itemIidIndexRowType,
                asList(itemRowType),
                InputPreservationOption.DISCARD_INPUT
            ),
            Arrays.asList(itemRowType)
        );

        // Build UPDATE-replacing project
        TPreparedExpression field0 = ExpressionGenerators.field(itemRowType, 0).getTPreparedExpression();
        TPreparedExpression field1 = ExpressionGenerators.field(itemRowType, 1).getTPreparedExpression();
        TPreparedExpression literal = ExpressionGenerators.literal(1000).getTPreparedExpression();
        TValidatedScalar plus = typesRegistryService().getScalarsResolver().get(
            "plus", asList(new TPreptimeValue(field0.resultType()), new TPreptimeValue(literal.resultType()))
        ).getOverload();
        TPreparedFunction prepFunc = new TPreparedFunction(
            plus, plus.resultType().fixed(false), Arrays.asList(field0, literal)
        );

        // Buffer, delete, insert scan
        final Operator update = insert_Returning(
            project_Table(
                buffer_Default(
                    delete_Returning(
                        pkScan, false
                    ),
                    itemRowType
                ),
                itemRowType,
                itemRowType,
                asList(prepFunc, field1)
            )
        );

        int modified = 0;
        Cursor updateCursor = cursor(update, queryContext, queryBindings);
        updateCursor.open();
        while(updateCursor.next() != null) {
            ++modified;
        }
        updateCursor.close();
        assertEquals("rows modified", 8, modified);

        Cursor executable = cursor(pkScan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(itemRowType, 1111L, 11L),
            row(itemRowType, 1112L, 11L),
            row(itemRowType, 1121L, 12L),
            row(itemRowType, 1122L, 12L),
            row(itemRowType, 1211L, 21L),
            row(itemRowType, 1212L, 21L),
            row(itemRowType, 1221L, 22L),
            row(itemRowType, 1222L, 22L),
        };
        compareRows(expected, executable);
    }


    @Test
    public void updateCustomer() {
        use(db);
        doUpdate();
        compareRows(
                array(Row.class,
                      row(customerRowType, 1L, "xyz"),
                      row(customerRowType, 2L, "zzz")
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
    public void updateCustomerCheckNameIndex() {
        use(db);
        doUpdate();
        compareRows(
                array(Row.class,
                      row(customerNameIndexRowType, "xyz", 1L),
                      row(customerNameIndexRowType, "zzz", 2L)
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
    public void updateCustomerCheckNameItemOidGroupIndex() {
        use(db);
        doUpdate();
        compareRows(
                array(Row.class,
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 111L),
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 112L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 121L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 122L),
                      row(customerNameItemOidIndexRowType, "zzz", 21L, 2L, 21L, 211L),
                      row(customerNameItemOidIndexRowType, "zzz", 21L, 2L, 21L, 212L),
                      row(customerNameItemOidIndexRowType, "zzz", 22L, 2L, 22L, 221L),
                      row(customerNameItemOidIndexRowType, "zzz", 22L, 2L, 22L, 222L)
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

    private void doUpdate() {
        Row[] rows = {
                row(customerRowType, new Object[]{2, "abc"})
        };
        UpdateFunction updateFunction = new UpdateFunction() {
            @Override
            public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
                return row(customerRowType, 2L, "zzz");
            }
        };
        Operator insertPlan = update_Returning(rowsToValueScan(rows), updateFunction);
        List<Row> result = runPlan(queryContext, queryBindings, insertPlan);
        assertEquals("rows touched", rows.length, result.size());
    }
}
