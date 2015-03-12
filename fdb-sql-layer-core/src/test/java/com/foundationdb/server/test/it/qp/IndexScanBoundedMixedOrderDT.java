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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.server.test.it.qp.IndexScanUnboundedMixedOrderDT.OrderByOptions;
import com.foundationdb.util.Strings;

import org.junit.ComparisonFailure;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;


@RunWith(SelectedParameterizedRunner.class)
public class IndexScanBoundedMixedOrderDT extends IndexScanUnboundedMixedOrderDT {

    private List<Integer> loBounds;
    private List<Integer> hiBounds;
    private List<Boolean> loInclusive;
    private List<Boolean> hiInclusive;
    private List<Boolean> skipped;


    public IndexScanBoundedMixedOrderDT(String name, List<OrderByOptions> orderings, List<Integer> loBounds, List<Integer> hiBounds,
                                        List<Boolean> loInclusive, List<Boolean> hiInclusive, List<Boolean> skipped) {
        super(name, orderings);
        this.loBounds = loBounds;
        this.hiBounds = hiBounds;
        this.loInclusive = loInclusive;
        this.hiInclusive = hiInclusive;
        this.skipped = skipped;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void compare(List<List<Integer>> expectedResults, List<List<?>> results) {
        int eSize = expectedResults.size();
        int aSize = results.size();
        boolean match = true;
        boolean bounds = true;
        for(int i = 0; match && bounds && i < Math.min(eSize, aSize); ++i) {
            match = rowComparator.compare(expectedResults.get(i), (List<Integer>)results.get(i)) == 0;
            bounds = withinBounds(results.get(i), loBounds, loInclusive, hiBounds, hiInclusive, skipped);
        }
        if(!match || !bounds || (eSize != aSize)) {
            throw new ComparisonFailure("row mismatch", Strings.join(expectedResults), Strings.join(results));
        }
    }

    @Override
    protected String buildQuery() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT ");
        for(int i = 0; i < TOTAL_COLS; ++i) {
            if(i > 0) {
                queryBuilder.append(", ");
            }
            queryBuilder.append(COLUMNS.get(i));
        }
        queryBuilder.append(" FROM ");
        queryBuilder.append(TABLE_NAME);

        queryBuilder.append(buildConditions(loBounds, loInclusive, hiBounds, hiInclusive, skipped));
        queryBuilder.append(buildOrderings(orderings));
        return queryBuilder.toString();
    }

    private static StringBuilder buildConditions(List<Integer> loBounds, List<Boolean> loInclusive,
            List<Integer> hiBounds, List<Boolean> hiInclusive, List<Boolean> skipped) {
        // TODO: Add more complex conditions, e.g., (t0 IS NULL OR (t0 > 10 AND t0 < 100))
        StringBuilder conditionsBuilder = new StringBuilder();
        boolean hasConditions = false;
        for (int i = 0; i < loBounds.size(); i++) {
            if (!skipped.get(i)) {
                if (hasConditions) {
                    conditionsBuilder.append(" AND ");
                } else {
                    conditionsBuilder.append(" WHERE ");
                    hasConditions = true;
                }

                if (loBounds.get(i) == null || hiBounds.get(i) == null) {
                    conditionsBuilder.append(COLUMNS.get(i) + " IS NULL");
                    continue;
                }                

                String lower_bound, upper_bound;
                lower_bound = Integer.toString(loBounds.get(i));
                upper_bound = Integer.toString(hiBounds.get(i));

                if (loInclusive.get(i)) {
                    conditionsBuilder.append(COLUMNS.get(i) + " >= " + lower_bound + " AND ");
                } else {
                    conditionsBuilder.append(COLUMNS.get(i) + " > " + lower_bound + " AND ");
                }
                if (hiInclusive.get(i)) {
                    conditionsBuilder.append(COLUMNS.get(i) + " <= " + upper_bound);
                } else {
                    conditionsBuilder.append(COLUMNS.get(i) + " < " + upper_bound);
                }
            }
        }
        return conditionsBuilder;
    }

    private static StringBuilder buildOrderings(List<OrderByOptions> orderings) {
        StringBuilder orderingsBuilder = new StringBuilder();
        orderingsBuilder.append(" ORDER BY ");
        boolean firstOrdering = true;
        for (int i = 0; i < orderings.size(); i++) {
            String oStr = orderings.get(i).getOrderingString();
            if (oStr != null && firstOrdering) {
                orderingsBuilder.append(COLUMNS.get(i) + " " + oStr);
                firstOrdering = false;
            }
            else if (oStr != null) {
                orderingsBuilder.append(", " + COLUMNS.get(i) + " " + oStr);
            }
        }
        return orderingsBuilder;
    }

    @Override
    protected List<List<Integer>> expectedRows() {
        List<List<Integer>> newRows = super.expectedRows();
        return filterRows(newRows);
    }

    protected List<List<Integer>> filterRows(List<List<Integer>> newRows) {
        List<List<Integer>> expected = new ArrayList<>();
        for(List<Integer> row : newRows) {
            if (withinBounds(row, loBounds, loInclusive, hiBounds, hiInclusive, skipped)) {
                expected.add(row);
            }
        }
        return expected;
    }

    protected static Boolean withinBounds(List<?> values, List<Integer> loBounds, List<Boolean> loInclusive,
            List<Integer> hiBounds, List<Boolean> hiInclusive, List<Boolean> skipped) {
        for (int i = 0; i < values.size(); i++) {
            if (!skipped.get(i) && !(withinBounds((Integer)values.get(i), loBounds.get(i), loInclusive.get(i), 
                    hiBounds.get(i), hiInclusive.get(i)))) {
                return false;
            }
        }
        return true;
    }

    protected static Boolean withinBounds(Integer value, Integer loBound, Boolean loInclusive, Integer hiBound,
            Boolean hiInclusive) {
        if (value == null && (loBound == null || hiBound == null)) {
            return true;
        }
        if (value == null || loBound == null || hiBound == null) {
            return false;
        }
        Boolean loCheck = loInclusive;
        if (value < loBound) {
            loCheck = false;
        }
        if (value > loBound) {
            loCheck = true;
        }
        Boolean hiCheck = hiInclusive;
        if (value > hiBound) {
            hiCheck = false;
        }
        if (value < hiBound) {
            hiCheck = true;
        }
        return loCheck && hiCheck;
    }

    @Parameters(name="{0}")
    public static List<Object[]> params() throws Exception {
        Random random = classRandom.reset();
        Collection<List<OrderByOptions>> orderByPerms = IndexScanUnboundedMixedOrderDT.orderByPermutations();
        List<Object[]> params = new ArrayList<>();
        for(List<OrderByOptions> ordering : orderByPerms) {
            boolean nonEmpty = false;
            for(OrderByOptions o : ordering) {
                if(o.getOrderingString() != null) {
                    nonEmpty = true;
                    break;
                }
            }
            if(nonEmpty) {
                List<Integer> loBounds = getLowerBounds(MIN_VALUE, MAX_VALUE, TOTAL_COLS, random);
                List<Integer> hiBounds = getUpperBounds(MIN_VALUE, MAX_VALUE, TOTAL_COLS, loBounds, random);
                List<Boolean> loInclusive = getBooleans(TOTAL_COLS, random);
                List<Boolean> hiInclusive = getBooleans(TOTAL_COLS, random);
                List<Boolean> skipped = getBooleans(TOTAL_COLS, random);
                String name = makeTestName(ordering, loBounds, loInclusive, hiBounds, hiInclusive, skipped);
                Object[] param = new Object[]{ name, ordering, loBounds, hiBounds, loInclusive, hiInclusive, skipped };
                params.add(param);
            }
        }
        return params;
    }

    protected static List<Integer> getLowerBounds(int min, int max, int cols, Random r) {
        List<Integer> bounds = new ArrayList<Integer>();
        for (int i = 0; i < cols; i++) {
            if (r.nextInt(10) == 1) {
                bounds.add(null);
            } else {
                bounds.add(r.nextInt(max - min) + min);
            }
        }
        return bounds;
    }

    protected static List<Integer> getUpperBounds(int min, int max, int cols, List<Integer> lowerBounds, Random r) {
        List<Integer> bounds = new ArrayList<Integer>();
        for (int i = 0; i < cols; i++) {
            if (r.nextInt(10) == 1) {
                bounds.add(null);
            } else if (lowerBounds.get(i) == null) {
                bounds.add(r.nextInt(max - min) + min);
            } else {
                bounds.add(r.nextInt(max - lowerBounds.get(i) + 1) + lowerBounds.get(i) - 1);
            }
        }
        return bounds;
    }

    protected static List<Boolean> getBooleans(int num, Random r) {
        List<Boolean> bounds = new ArrayList<Boolean>();
        for (int i = 0; i < num; i++) {
            bounds.add(r.nextBoolean());
        }
        return bounds;
    }

    protected static String makeTestName(List<OrderByOptions> orderings, List<Integer> loBounds, List<Boolean> loInclusive, 
            List<Integer> hiBounds, List<Boolean> hiInclusive, List<Boolean> skipped) {
        StringBuilder sb = new StringBuilder(IndexScanUnboundedMixedOrderDT.makeTestName(orderings));
        sb.append(" ");
        sb.append(buildConditions(loBounds, loInclusive, hiBounds, hiInclusive, skipped));
        return sb.toString();
    }
}
