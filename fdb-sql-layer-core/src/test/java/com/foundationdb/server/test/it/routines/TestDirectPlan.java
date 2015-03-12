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
package com.foundationdb.server.test.it.routines;

import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;

import java.sql.Types;

import java.util.Arrays;
import java.util.List;

/** A loadable direct object plan.
 * <code><pre>
CALL sqlj.install_jar('target/fdb-sql-layer-x.y.z-tests.jar', 'testjar', 0);
CREATE PROCEDURE test_direct(IN n BIGINT) LANGUAGE java PARAMETER STYLE foundationdb_loadable_plan EXTERNAL NAME 'testjar:com.foundationdb.server.test.it.routines.TestDirectPlan';
CALL test_direct(10);
 * </pre></code> 
 */
public class TestDirectPlan extends LoadableDirectObjectPlan
{
    @Override
    public DirectObjectPlan plan()
    {
        return new DirectObjectPlan() {
                @Override
                public DirectObjectCursor cursor(QueryContext context, QueryBindings bindings) {
                    return new TestDirectObjectCursor(context, bindings);
                }
            };
    }

    public static class TestDirectObjectCursor extends DirectObjectCursor {
        private QueryContext context;
        private QueryBindings bindings;
        private long i, n;

        public TestDirectObjectCursor(QueryContext context, QueryBindings bindings) {
            this.context = context;
            this.bindings = bindings;
        }

        @Override
        public void open() {
            i = 0;
            //n = bindings.getValue(0).getLong();
            n = bindings.getValue(0).getInt64();
        }

        @Override
        public List<Long> next() {
            if (i >= n)
                return null;
            return Arrays.asList(i++);
        }

        @Override
        public void close() {
        }
    }

    @Override
    public List<String> columnNames() {
        return NAMES;
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final List<String> NAMES = Arrays.asList("i");
    private static final int[] TYPES = new int[] { Types.INTEGER };
}
