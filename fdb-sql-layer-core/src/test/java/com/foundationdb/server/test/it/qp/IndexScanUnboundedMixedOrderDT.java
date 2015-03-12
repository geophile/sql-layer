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
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.sql.embedded.EmbeddedJDBCITBase;
import com.foundationdb.util.RandomRule;
import com.foundationdb.util.Strings;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.ComparisonFailure;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(SelectedParameterizedRunner.class)
public class IndexScanUnboundedMixedOrderDT extends EmbeddedJDBCITBase
{
    protected static final String TABLE_NAME ="t";
    protected static final String INDEX_NAME = "idx";
    protected static final List<String> COLUMNS = Arrays.asList("t0", "t1", "t2", "t3");
    protected static final Integer TOTAL_ROWS = 100;
    protected static final Integer TOTAL_COLS = COLUMNS.size();
    static final Integer MAX_VALUE = 100;
    static final Integer MIN_VALUE = 0;


    @SuppressWarnings("unchecked")
    static final Comparator ASC_COMPARATOR = new Comparator()
    {
        @Override
        public int compare(Object o1, Object o2) {
            if(o1 == null) {
                return (o2 == null) ? 0 : -1;
            }
            if(o2 == null) {
                return 1;
            }
            return ((Comparable)o1).compareTo(o2);
        }
    };

    @SuppressWarnings("unchecked")
    static final Comparator DESC_COMPARATOR = new Comparator()
    {
        @Override
        public int compare(Object o1, Object o2) {
            return - ASC_COMPARATOR.compare(o1, o2);
        }
    };


    static enum OrderByOptions {
        NONE,
        ASC,
        DESC
        ;

        @SuppressWarnings("unchecked")
        public <T> Comparator<T> getComparator(Class<T> clazz) {
            switch(this) {
                case NONE: return null;
                case ASC: return ASC_COMPARATOR;
                case DESC: return DESC_COMPARATOR;
                default: throw new IllegalStateException(this.name());
            }
        }

        public String getOrderingString() {
            switch(this) {
                case NONE: return null;
                default: return name();
            }
        }
    }

    static class IndexComparison<T> {
        private final int index;
        private final Comparator<T> comp;

        public IndexComparison(int index, Comparator<T> comp) {
            this.index = index;
            this.comp = comp;
        }
    }

    static class ListComparator<T> implements Comparator<List<T>>
    {
        public final List<IndexComparison<T>> comps;

        public ListComparator(List<IndexComparison<T>> comps) {
            this.comps = comps;
        }

        @Override
        public int compare(List<T> a, List<T> b) {
            assert a.size() == b.size();
            for(IndexComparison<T> c : comps) {
                int i = c.index;
                int r = c.comp.compare(a.get(i), b.get(i));
                if(r != 0) {
                    return r;
                }
            }
            return 0;
        }
    }

    @ClassRule
    public static final RandomRule classRandom = new RandomRule();
    @Rule
    public final RandomRule randomRule = classRandom;

    protected final List<OrderByOptions> orderings;
    protected final List<List<Integer>> DB;
    protected final ListComparator<Integer> rowComparator;
    protected String query;

    public IndexScanUnboundedMixedOrderDT(String name, List<OrderByOptions> orderings) {
        this.orderings = orderings;
        this.rowComparator = buildListComparator(orderings);
        this.DB = buildDB(randomRule.getRandom());
    }

    private static ListComparator<Integer> buildListComparator(List<OrderByOptions> orderings) {
        List<IndexComparison<Integer>> comps = new ArrayList<>();
        for(int i = 0; i < orderings.size(); ++i) {
            Comparator<Integer> comp = orderings.get(i).getComparator(Integer.class);
            if(comp != null) {
                comps.add(new IndexComparison<>(i, comp));
            }
        }
        return new ListComparator<>(comps);
    }

    private static List<List<Integer>> buildDB(Random r) {
        List<List<Integer>> db = new ArrayList<>();
        for (int i = 0; i < TOTAL_ROWS; i++) {
            List<Integer> row = new ArrayList<>();
            for (int j = 0; j < TOTAL_COLS; j++) {
                int next = r.nextInt(MAX_VALUE + 10);
                row.add(next > MAX_VALUE ? null : next);
            }
            db.add(row);
        }
        return db;
    }

    @Before
    public void setup() {
        sql("CREATE TABLE " + TABLE_NAME + "(id SERIAL PRIMARY KEY, t0 INT, t1 INT, t2 INT, t3 INT)");
        sql("CREATE INDEX " + INDEX_NAME + " ON t(t0, t1, t2, t3)");
        StringBuilder sb = new StringBuilder("INSERT INTO " + TABLE_NAME + "(t0,t1,t2,t3) VALUES ");
        for(int i = 0; i < DB.size(); ++i) {
            if(i > 0) {
                sb.append(", ");
            }
            sb.append('(').append(Strings.join(DB.get(i), ",")).append(')');
        }
        sql(sb.toString());
    }

    @Test
    public void testQuery() {
        this.query = buildQuery();
        List<List<?>> results = sql(query);
        compare(expectedRows(), results);
    }

    @SuppressWarnings("unchecked")
    protected void compare(List<List<Integer>> expectedResults, List<List<?>> results) {
        int eSize = expectedResults.size();
        int aSize = results.size();
        boolean match = true;
        for(int i = 0; match && i < Math.min(eSize, aSize); ++i) {
            match = rowComparator.compare(expectedResults.get(i), (List<Integer>)results.get(i)) == 0;
        }
        if(!match || (eSize != aSize)) {
            throw new ComparisonFailure("row mismatch", Strings.join(expectedResults), Strings.join(results));
        }
    }

    protected String buildQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for(int i = 0; i < TOTAL_COLS; ++i) {
            if(i > 0) {
                sb.append(", ");
            }
            sb.append(COLUMNS.get(i));
        }
        sb.append(" FROM ");
        sb.append(TABLE_NAME);
        boolean first = true;
        for(int i = 0; i < orderings.size(); i++) {
            String oStr = orderings.get(i).getOrderingString();
            if(oStr != null) {
                if(first) {
                    first = false;
                    sb.append(" ORDER BY ");
                } else {
                    sb.append(", ");
                }
                sb.append(COLUMNS.get(i));
                sb.append(" ");
                sb.append(oStr);
            }
        }
        return sb.toString();
    }

    protected List<List<Integer>> expectedRows() {
        List<List<Integer>> sorted = new ArrayList<>(DB);
        Collections.sort(sorted, rowComparator);
        return sorted;
    }

    public static Collection<List<OrderByOptions>> orderByPermutations() {
        List<Set<OrderByOptions>> optSets = new ArrayList<>();
        for(int i = 0; i < TOTAL_COLS; ++i) {
            optSets.add(EnumSet.allOf(OrderByOptions.class));
        }
        return Sets.cartesianProduct(optSets);
    }

    @Parameters(name="{0}")
    public static List<Object[]> params() throws Exception {
        List<Object[]> params = new ArrayList<>();
        for(List<OrderByOptions> p : orderByPermutations()) {
            String name = makeTestName(p);
            params.add(new Object[]{ name, p });
        }
        return params;
    }

    protected static String makeTestName(List<OrderByOptions> orderings) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < orderings.size(); ++i) {
            String oStr = orderings.get(i).getOrderingString();
            if(oStr != null) {
                if(sb.length() > 0) {
                    sb.append('_');
                }
                sb.append(COLUMNS.get(i));
                sb.append('_').append(oStr);
            }
        }
        if(sb.length() == 0) {
            sb.append("no_order");
        }
        return sb.toString();
    }
}
