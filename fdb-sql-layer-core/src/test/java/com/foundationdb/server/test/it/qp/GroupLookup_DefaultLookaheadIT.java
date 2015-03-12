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
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
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

public class GroupLookup_DefaultLookaheadIT extends GroupLookup_DefaultIT
{
    protected void moreDB() {
        Row[] daves = new Row[]{
            row(order, 23L, 2L, "dave"),
            row(order, 24L, 2L, "dave"),
            row(order, 25L, 2L, "dave")};
        writeRows(daves);
    }

    @Override
    protected boolean pipelineMap() {
        return true;
    }

    @Override
    protected int lookaheadQuantum() {
        return 4;
    }

    @Test
    public void testAncestorLookupSimple()
    {
        moreDB();
        Operator plan =
            groupLookup_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                coi,
                orderRowType,
                Collections.singleton(customerRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 2L, "foundation"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testAncestorLookupMap()
    {
        moreDB();
        RowType cidValueRowType = schema.newValuesType(MNumeric.INT.instance(true));
        List<ExpressionGenerator> cidExprs = Arrays.asList(boundField(cidValueRowType, 1, 0));
        IndexBound cidBound =
            new IndexBound(
                new RowBasedUnboundExpressions(orderCidIndexRowType, cidExprs, true),
                new SetColumnSelector(0));
        IndexKeyRange cidRange = IndexKeyRange.bounded(orderCidIndexRowType, cidBound, true, cidBound, true);
        Operator plan =
            map_NestedLoops(
                valuesScan_Default(
                    bindableExpressions(intRow(cidValueRowType, 3),
                                        intRow(cidValueRowType, 2),
                                        intRow(cidValueRowType, 10)),
                    cidValueRowType),
                groupLookup_Default(
                    indexScan_Default(orderCidIndexRowType, cidRange, ordering(orderCidIndexRowType), IndexScanSelector.leftJoinAfter(orderCidIndexRowType.index(), orderRowType.table()), lookaheadQuantum()),
                    coi,
                    orderCidIndexRowType,
                    Arrays.asList(customerRowType, orderRowType),
                    InputPreservationOption.DISCARD_INPUT,
                    lookaheadQuantum()),
                1, pipelineMap(), 1);
        Row[] expected = new Row[]{
            row(orderRowType, 31L, 3L, "peter"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 22L, 2L, "jack"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 23L, 2L, "dave"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 24L, 2L, "dave"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 25L, 2L, "dave"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testAncestorLookupMap2()
    {
        moreDB();
        RowType cidValueRowType = schema.newValuesType(MNumeric.INT.instance(true));
        List<ExpressionGenerator> cidExprs = Arrays.asList(boundField(cidValueRowType, 1, 0));
        IndexBound cidBound =
            new IndexBound(
                new RowBasedUnboundExpressions(orderCidIndexRowType, cidExprs, true),
                new SetColumnSelector(0));
        IndexKeyRange cidRange = IndexKeyRange.bounded(orderCidIndexRowType, cidBound, true, cidBound, true);
        Operator plan =
            map_NestedLoops(
                valuesScan_Default(
                    bindableExpressions(intRow(cidValueRowType, -1),
                                        intRow(cidValueRowType, -2)),
                    cidValueRowType),
                map_NestedLoops(
                    valuesScan_Default(
                        bindableExpressions(intRow(cidValueRowType, 3),
                                            intRow(cidValueRowType, 2),
                                            intRow(cidValueRowType, 10)),
                        cidValueRowType),
                    groupLookup_Default(
                        indexScan_Default(orderCidIndexRowType, cidRange, ordering(orderCidIndexRowType), IndexScanSelector.leftJoinAfter(orderCidIndexRowType.index(), orderRowType.table()), lookaheadQuantum()),
                        coi,
                        orderCidIndexRowType,
                        Arrays.asList(customerRowType, orderRowType),
                        InputPreservationOption.DISCARD_INPUT,
                        lookaheadQuantum()),
                    1, pipelineMap(), 2),
                0, pipelineMap(), 1);
        Row[] expected = new Row[]{
            row(orderRowType, 31L, 3L, "peter"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 22L, 2L, "jack"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 23L, 2L, "dave"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 24L, 2L, "dave"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 25L, 2L, "dave"),
            row(orderRowType, 31L, 3L, "peter"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 22L, 2L, "jack"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 23L, 2L, "dave"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 24L, 2L, "dave"),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 25L, 2L, "dave"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    // For use by this class

    private Row intRow(RowType rowType, int x)
    {
        List<TPreparedExpression> pExpressions = Arrays.<TPreparedExpression>asList(new TPreparedLiteral(MNumeric.INT.instance(false), new Value(MNumeric.INT.instance(false), x)));
        return new ExpressionRow(rowType, queryContext, queryBindings, pExpressions);
    }

    private Collection<? extends BindableRow> bindableExpressions(Row... rows) {
        List<BindableRow> result = new ArrayList<>();
        for (Row row : rows) {
            result.add(BindableRow.of(row));
        }
        return result;
    }

    private Ordering ordering(IndexRowType indexRowType) {
        Ordering ordering = new Ordering();
        for (int i = 0; i < indexRowType.nFields(); i++) {
            ordering.append(field(indexRowType, i), true);
        }
        return ordering;
    }

}
