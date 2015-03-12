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

import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHKey;
import com.foundationdb.qp.rowtype.HKeyRowType;
import com.foundationdb.server.error.AkibanInternalException;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FullTextCursor extends RowCursorImpl
{
    private final QueryContext context;
    private final HKeyRowType rowType;
    private final SearcherManager searcherManager;
    private final Query query;
    private final int limit;
    //private final StoreAdapter adapter;
    private IndexSearcher searcher;
    private TopDocs results = null;
    private int position;

    public static final Sort SORT = new Sort(SortField.FIELD_SCORE,
                                             new SortField(IndexedField.KEY_FIELD,
                                                           SortField.Type.STRING));

    private static final Logger logger = LoggerFactory.getLogger(FullTextCursor.class);

    public FullTextCursor(QueryContext context, HKeyRowType rowType, 
                          SearcherManager searcherManager, Query query, int limit) {
        this.context = context;
        this.rowType = rowType;
        this.searcherManager = searcherManager;
        this.query = query;
        this.limit = limit;
        //adapter = context.getStore();
        searcher = searcherManager.acquire();
    }

    @Override
    public void open() {
        super.open();
        logger.debug("FullTextCursor: open {}", query);
        if (query == null) {
            setIdle();
        }
        else {
            try {
                results = searcher.search(query, limit, SORT);
            }
            catch (IOException ex) {
                throw new AkibanInternalException("Error searching index", ex);
            }
        }
        position = 0;
    }
    
    @Override
    public Row next() {
        CursorLifecycle.checkIdleOrActive(this);
        if (isIdle())
            return null;
        if (position >= results.scoreDocs.length) {
            setIdle();
            results = null;
            return null;
        }
        Document doc;
        try {
            doc = searcher.doc(results.scoreDocs[position++].doc);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error reading document", ex);
        }
        Row row = toHkeyRow(doc.get(IndexedField.KEY_FIELD));
        logger.debug("FullTextCursor: yield {}", row);
        return row;
    }

    @Override
    public void close() {
        super.close();
        results = null;
        try {
            searcherManager.release(searcher);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error releasing searcher", ex);
        }
        searcher = null;
    }

    /* Allocate a new <code>HKey</code> and copy the given
     * key bytes into it. */
    protected Row toHkeyRow(String encoded) {
        HKey hkey = context.getStore().getKeyCreator().newHKey(rowType.hKey());
        byte decodedBytes[] = RowIndexer.decodeString(encoded);
        hkey.copyFrom(decodedBytes);
        return (Row)(ValuesHKey)hkey;
    }
}
