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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.API.IntersectOption;
import com.foundationdb.qp.operator.API.JoinType;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.types.TKeyComparable;

import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;

/**
 * Testing nested intersects across differing types (BIGINT -> INT).
 * Intersect_Ordered wasn't initially using the TComparisons list when comparing the skip row, leading to an error.
 */
public class Intersect_Ordered_BigIntIntIT extends OperatorITBase
{
    private static final String SCHEMA = "test";
    private static final String REPORTS = "reports";
    private static final String METADATA = "metadata";

    private int rid;
    private int mid;
    @Override
    protected void setupCreateSchema()
    {
        rid = createTable(SCHEMA, REPORTS,
                "id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(255)");
        createIndex(SCHEMA, REPORTS, "name", "name");
        mid = createTable(SCHEMA, METADATA,
                              "id BIGINT NOT NULL PRIMARY KEY, report_id INT, name VARCHAR(255), value VARCHAR(255)",
                              "GROUPING FOREIGN KEY(report_id) REFERENCES reports(id)");
        createIndex(SCHEMA, METADATA, "name_value", "name", "value");
    }
    @Override
    protected void setupPostCreateSchema()
    {
        writeRows(
                row(rid, 1, "foo"),
                row(mid, 10, 1, "x", "one"),
                row(rid, 2, "bar"),
                row(mid, 20, 2, "x", "one"),
                row(rid, 3, "foo"),
                row(mid, 30, 3, "x", "one"),
                row(mid, 31, 3, "y", "two"),
                row(rid, 4, "zap"),
                row(mid, 40, 4, "x", "one"),
                row(mid, 41, 4, "y", "two")
            );
    }    
    
    @Test
    public void test() {
        IndexRowType rIndex = schema.indexRowType(ais().getTable(rid).getIndex("name"));
        IndexRowType mdIndex = schema.indexRowType(ais().getTable(mid).getIndex("name_value"));

        /*
        Build something like:
          Intersect_Ordered(skip 2 left, skip 2 right, compare 1)
            Intersect_Ordered(skip 2 left, skip 1 right, compare 1)
              IndexScan_Default(Index(metadata.name_value), name = 'x', value = 'one')
              IndexScan_Default(Index(reports.name), name = 'foo')
            IndexScan_Default(Index(metadata.name_value), name = 'y', value = 'two')
         */

        IndexBound fooBound = new IndexBound(testRow(rIndex, "foo", null), new SetColumnSelector(0));
        Operator fooScan = API.indexScan_Default(rIndex,
                                                 false,
                                                 IndexKeyRange.bounded(rIndex, fooBound, true, fooBound, true));

        IndexBound xOneBound = new IndexBound(testRow(mdIndex, "x", "one", null, null), new SetColumnSelector(0, 1));
        Operator xOneScan = API.indexScan_Default(mdIndex,
                                                   false,
                                                   IndexKeyRange.bounded(mdIndex, xOneBound, true, xOneBound, true));

        IndexBound yTwoBound = new IndexBound(testRow(mdIndex, "y", "two", null, null), new SetColumnSelector(0, 1));
        Operator yTwoScan = API.indexScan_Default(mdIndex,
                                                  false,
                                                  IndexKeyRange.bounded(mdIndex, yTwoBound, true, yTwoBound, true));

        TKeyComparable comparableIntBigint = typesRegistryService().getKeyComparable(
            ais().getTable(mid).getColumn("report_id").getType().typeClass(),
            ais().getTable(rid).getColumn("id").getType().typeClass()
        );

        Operator innerIntersect = API.intersect_Ordered(
                xOneScan,
                fooScan,
                mdIndex,
                rIndex,
                mdIndex.index().getAllColumns().size() - 2,
                rIndex.index().getAllColumns().size() - 1,
                new boolean[]{true},
                JoinType.INNER_JOIN,
                EnumSet.of(IntersectOption.OUTPUT_LEFT, IntersectOption.SKIP_SCAN),
                Arrays.asList(comparableIntBigint.getComparison()),
                true
        );

        Operator outerIntersect = API.intersect_Ordered(
                innerIntersect,
                yTwoScan,
                mdIndex,
                mdIndex,
                mdIndex.index().getAllColumns().size() - 2,
                mdIndex.index().getAllColumns().size() - 2,
                new boolean[]{true},
                JoinType.INNER_JOIN,
                EnumSet.of(IntersectOption.OUTPUT_LEFT, IntersectOption.SKIP_SCAN),
                null,
                true
        );

        QueryContext context = new SimpleQueryContext(newStoreAdapter());
        Cursor cursor = API.cursor(outerIntersect, context, context.createBindings());
        compareRows(
            new Row[] {
                testRow(mdIndex, "x", "one", 3, 30)
            },
            cursor
        );
    }
}
