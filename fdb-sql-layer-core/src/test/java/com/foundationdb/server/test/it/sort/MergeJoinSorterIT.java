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
package com.foundationdb.server.test.it.sort;

import java.util.HashMap;
import java.util.Map;

import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.API.SortOption;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.config.TestConfigService;
import com.foundationdb.util.tap.InOutTap;

public class MergeJoinSorterIT extends SorterITBase {

    @Override
    public Map<String,String> startupConfigProperties() {
        Map<String,String> props = new HashMap<>();
        props.putAll(super.startupConfigProperties());

        props.put("fdbsql.tmp_dir", TestConfigService.dataDirectory().getAbsolutePath());
        return props;
    }

    @Override
    public Sorter createSorter(QueryContext context, QueryBindings bindings,
            Cursor input, RowType rowType, Ordering ordering,
            SortOption sortOption, InOutTap loadTap) {
        return new MergeJoinSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
    }

}
