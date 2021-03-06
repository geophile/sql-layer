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


import com.foundationdb.qp.expression.ExpressionRow;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.expression.RowBasedUnboundExpressions;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.*;

public class Map_NestedLoopsIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
        Row[] db = new Row[]{
            row(customer, 1L, "northbridge"), // two orders, two addresses
            row(order, 100L, 1L, "ori"),
            row(order, 101L, 1L, "ori"),
            row(address, 1000L, 1L, "111 1000 st"),
            row(address, 1001L, 1L, "111 1001 st"),
            row(customer, 2L, "foundation"), // two orders, one address
            row(order, 200L, 2L, "david"),
            row(order, 201L, 2L, "david"),
            row(address, 2000L, 2L, "222 2000 st"),
            row(customer, 3L, "matrix"), // one order, two addresses
            row(order, 300L, 3L, "tom"),
            row(address, 3000L, 3L, "333 3000 st"),
            row(address, 3001L, 3L, "333 3001 st"),
            row(customer, 4L, "atlas"), // two orders, no addresses
            row(order, 400L, 4L, "jack"),
            row(order, 401L, 4L, "jack"),
            row(customer, 5L, "highland"), // no orders, two addresses
            row(address, 5000L, 5L, "555 5000 st"),
            row(address, 5001L, 5L, "555 5001 st"),
            row(customer, 6L, "flybridge"), // no orders or addresses
            // Add a few items to test Product_ByRun rejecting unexpected input. All other tests remove these items.
            row(item, 1000L, 100L),
            row(item, 1001L, 100L),
            row(item, 1010L, 101L),
            row(item, 1011L, 101L),
            row(item, 2000L, 200L),
            row(item, 2001L, 200L),
            row(item, 2010L, 201L),
            row(item, 2011L, 201L),
            row(item, 3000L, 300L),
            row(item, 3001L, 300L),
            row(item, 4000L, 400L),
            row(item, 4001L, 400L),
            row(item, 4010L, 401L),
            row(item, 4011L, 401L),
        };
        use(db);
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testLeftInputNull()
    {
        map_NestedLoops(null, groupScan_Default(coi), 0, pipelineMap(), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRightInputNull()
    {
        map_NestedLoops(groupScan_Default(coi), null, 0, pipelineMap(), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeInputBindingPosition()
    {
        map_NestedLoops(groupScan_Default(coi), groupScan_Default(coi), -1, pipelineMap(), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonPositiveDepth()
    {
        map_NestedLoops(groupScan_Default(coi), groupScan_Default(coi), 0, true,0);
    }

    // Test operator execution

    @Test
    public void testIndexLookup()
    {
        Operator plan =
            map_NestedLoops(
                indexScan_Default(itemOidIndexRowType, false),
                ancestorLookup_Nested(coi, itemOidIndexRowType, Collections.singleton(itemRowType), 0, 1),
                0, pipelineMap(), 1);
        Row[] expected = new Row[]{
            row(itemRowType, 1000L, 100L),
            row(itemRowType, 1001L, 100L),
            row(itemRowType, 1010L, 101L),
            row(itemRowType, 1011L, 101L),
            row(itemRowType, 2000L, 200L),
            row(itemRowType, 2001L, 200L),
            row(itemRowType, 2010L, 201L),
            row(itemRowType, 2011L, 201L),
            row(itemRowType, 3000L, 300L),
            row(itemRowType, 3001L, 300L),
            row(itemRowType, 4000L, 400L),
            row(itemRowType, 4001L, 400L),
            row(itemRowType, 4010L, 401L),
            row(itemRowType, 4011L, 401L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testInnerJoin()
    {
        // customer order inner join, done as a general join
        Operator project =
            project_DefaultTest(
                select_HKeyOrdered(
                    filter_Default(
                        groupScan_Default(coi),
                        Collections.singleton(orderRowType)),
                    orderRowType,
                    compare(
                            field(orderRowType, 1) /* order.cid */,
                            Comparison.EQ,
                            boundField(customerRowType, 0, 0) /* customer.cid */, castResolver())),
                orderRowType,
                Arrays.asList(boundField(customerRowType, 0, 0) /* customer.cid */, field(orderRowType, 0) /* order.oid */));
        Operator plan =
            map_NestedLoops(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                project,
                0, pipelineMap(), 1);
        RowType projectRowType = project.rowType();
        Row[] expected = new Row[]{
            row(projectRowType, 1L, 100L),
            row(projectRowType, 1L, 101L),
            row(projectRowType, 2L, 200L),
            row(projectRowType, 2L, 201L),
            row(projectRowType, 3L, 300L),
            row(projectRowType, 4L, 400L),
            row(projectRowType, 4L, 401L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testOuterJoin()
    {
        // customer order outer join, done as a general join
        Operator project = project_DefaultTest(
            select_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                compare(
                        field(orderRowType, 1) /* order.cid */,
                        Comparison.EQ,
                        boundField(customerRowType, 0, 0) /* customer.cid */, castResolver())),
            orderRowType,
            Arrays.asList(boundField(customerRowType, 0, 0) /* customer.cid */, field(orderRowType, 0) /* order.oid */));
        RowType projectRowType = project.rowType();
        Operator plan =
            map_NestedLoops(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                ifEmpty_DefaultTest(project, projectRowType, Arrays.asList(boundField(customerRowType, 0, 0), literal(null)), InputPreservationOption.KEEP_INPUT),
                0, pipelineMap(), 1);
        Row[] expected = new Row[]{
            row(projectRowType, 1L, 100L),
            row(projectRowType, 1L, 101L),
            row(projectRowType, 2L, 200L),
            row(projectRowType, 2L, 201L),
            row(projectRowType, 3L, 300L),
            row(projectRowType, 4L, 400L),
            row(projectRowType, 4L, 401L),
            row(projectRowType, 5L, null),
            row(projectRowType, 6L, null),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            map_NestedLoops(
                indexScan_Default(itemOidIndexRowType, false),
                ancestorLookup_Nested(coi, itemOidIndexRowType, Collections.singleton(itemRowType), 0, 1),
                0, pipelineMap(), 1);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(itemRowType, 1000L, 100L),
                    row(itemRowType, 1001L, 100L),
                    row(itemRowType, 1010L, 101L),
                    row(itemRowType, 1011L, 101L),
                    row(itemRowType, 2000L, 200L),
                    row(itemRowType, 2001L, 200L),
                    row(itemRowType, 2010L, 201L),
                    row(itemRowType, 2011L, 201L),
                    row(itemRowType, 3000L, 300L),
                    row(itemRowType, 3001L, 300L),
                    row(itemRowType, 4000L, 400L),
                    row(itemRowType, 4001L, 400L),
                    row(itemRowType, 4010L, 401L),
                    row(itemRowType, 4011L, 401L),
                };
            }

            @Override
            public boolean reopenTopLevel() {
                // You cannot just re-open() a pipelined Map_NestedLoops, but you can
                // openTopLevel() it again.
                return pipelineMap();
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test
    // Inspired by bug 869396
    public void testIndexScanUnderMapNestedLoopsUsedAsInnerLoopOfAnotherMapNestedLoops()
    {
        RowType cidValueRowType = schema.newValuesType(MNumeric.INT.instance(true));
        List<ExpressionGenerator> expressions = Arrays.asList(boundField(cidValueRowType, 1, 0));
        IndexBound cidBound =
            new IndexBound(
                new RowBasedUnboundExpressions(customerCidIndexRowType, expressions, true),
                new SetColumnSelector(0));
        IndexKeyRange cidRange = IndexKeyRange.bounded(customerCidIndexRowType, cidBound, true, cidBound, true);
        Operator plan =
            map_NestedLoops(
                valuesScan_Default(
                        bindableExpressions(intRow(cidValueRowType, 1),
                                intRow(cidValueRowType, 2),
                                intRow(cidValueRowType, 3),
                                intRow(cidValueRowType, 4),
                                intRow(cidValueRowType, 5)),
                    cidValueRowType),
                map_NestedLoops(
                    indexScan_Default(customerCidIndexRowType, false, cidRange),
                    ancestorLookup_Nested(coi, customerCidIndexRowType, Collections.singleton(customerRowType), 0, 1),
                    0, pipelineMap(), 2),
                1, pipelineMap(), 1);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 3L, "matrix"),
            row(customerRowType, 4L, "atlas"),
            row(customerRowType, 5L, "highland"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDeepMapLimit()
    {
        RowType intRowType = schema.newValuesType(MNumeric.INT.instance(true));
        List<ExpressionGenerator> expressions = Arrays.asList(boundField(intRowType, 0, 0), boundField(intRowType, 1, 0), field(intRowType, 0));
        Operator inside = 
            project_DefaultTest(
                valuesScan_Default(
                    bindableExpressions(intRow(intRowType, 1),
                                        intRow(intRowType, 2),
                                        intRow(intRowType, 3)),
                    intRowType),
                intRowType, expressions);
        RowType insideRowType = inside.rowType();
        Operator plan =
            map_NestedLoops(
                valuesScan_Default(
                    bindableExpressions(intRow(intRowType, 100),
                                        intRow(intRowType, 200),
                                        intRow(intRowType, 300)),
                    intRowType),
                limit_Default(
                    map_NestedLoops(
                        valuesScan_Default(
                            bindableExpressions(intRow(intRowType, 10),
                                                intRow(intRowType, 20),
                                                intRow(intRowType, 30)),
                            intRowType),
                        inside,
                        1, pipelineMap(), 2),
                    2),
                0, pipelineMap(), 1);
        Row[] expected = new Row[]{
            row(insideRowType, 100L, 10L, 1L),
            row(insideRowType, 100L, 10L, 2L),
            row(insideRowType, 200L, 10L, 1L),
            row(insideRowType, 200L, 10L, 2L),
            row(insideRowType, 300L, 10L, 1L),
            row(insideRowType, 300L, 10L, 2L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testLeftDeepMap()
    {
        RowType intRowType = schema.newValuesType(MNumeric.INT.instance(true));
        List<ExpressionGenerator> outerExprs = Arrays.asList(boundField(intRowType, 0, 0), field(intRowType, 0));
        Operator middle = 
            project_DefaultTest(
                valuesScan_Default(
                    bindableExpressions(intRow(intRowType, 10),
                                        intRow(intRowType, 20)),
                    intRowType),
                intRowType, outerExprs);
        RowType outerRowType = middle.rowType();
        Operator outer =
            map_NestedLoops(
                valuesScan_Default(
                    bindableExpressions(intRow(intRowType, 100),
                                        intRow(intRowType, 200)),
                    intRowType),
                middle,
                0, pipelineMap(), 1);
        List<ExpressionGenerator> innerExprs = Arrays.asList(boundField(outerRowType, 1, 0), boundField(outerRowType, 1, 1), field(intRowType, 0));
        Operator inner =
            project_DefaultTest(
                valuesScan_Default(
                    bindableExpressions(intRow(intRowType, 1),
                                        intRow(intRowType, 2)),
                    intRowType),
                intRowType, innerExprs);
        RowType innerRowType = inner.rowType();
        Operator plan = map_NestedLoops(outer, inner, 1, pipelineMap(), 1);
        Row[] expected = new Row[]{
            row(innerRowType, 100L, 10L, 1L),
            row(innerRowType, 100L, 10L, 2L),
            row(innerRowType, 100L, 20L, 1L),
            row(innerRowType, 100L, 20L, 2L),
            row(innerRowType, 200L, 10L, 1L),
            row(innerRowType, 200L, 10L, 2L),
            row(innerRowType, 200L, 20L, 1L),
            row(innerRowType, 200L, 20L, 2L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private Row intRow(RowType rowType, int x)
    {
        List<TPreparedExpression> pExpressions;
        pExpressions = Arrays.asList((TPreparedExpression) new TPreparedLiteral(
                MNumeric.INT.instance(false), new Value(MNumeric.INT.instance(false), x)));
        return new ExpressionRow(rowType, queryContext, queryBindings, pExpressions);
    }

    private Collection<? extends BindableRow> bindableExpressions(Row... rows) {
        List<BindableRow> result = new ArrayList<>();
        for (Row row : rows) {
            result.add(BindableRow.of(row));
        }
        return result;
    }
}
