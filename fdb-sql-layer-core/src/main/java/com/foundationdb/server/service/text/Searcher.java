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
package com.foundationdb.server.service.text;

import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.HKeyRowType;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Searcher implements Closeable
{
    public static final int DEFAULT_LIMIT = Integer.MAX_VALUE;

    private final FullTextIndexShared index;
    private final SearcherManager searcherManager;

    public Searcher(FullTextIndexShared index, Analyzer analyzer) throws IOException {
        this.index = index;
        this.searcherManager = new SearcherManager(index.open(), new SearcherFactory());
    }

    public RowCursor search(QueryContext context, HKeyRowType rowType,
                            Query query, int limit)
            throws IOException {
        searcherManager.maybeRefresh(); // TODO: Move to better place.
        if (limit <= 0) limit = DEFAULT_LIMIT;
        return new FullTextCursor(context, rowType, searcherManager, query, limit);
    }

    @Override
    public void close() throws IOException {
        searcherManager.close();
    }

}
