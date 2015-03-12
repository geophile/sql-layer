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

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Index.IndexType;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.google.inject.Inject;
import org.apache.lucene.search.Query;

import java.util.Collection;

public class ThrowingFullTextService implements Service, FullTextIndexService, TableListener
{
    private static final RuntimeException EX = new UnsupportedSQLException("FULL_TEXT indexing not supported");

    private final ListenerService listenerService;

    @Inject
    public ThrowingFullTextService(ListenerService listenerService) {
        this.listenerService = listenerService;
    }


    //
    // Service
    //

    @Override
    public void start() {
        listenerService.registerTableListener(this);
    }

    @Override
    public void stop() {
        listenerService.deregisterTableListener(this);
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // FullTextIndexService
    //

    @Override
    public RowCursor searchIndex(QueryContext context, IndexName name, Query query, int limit) {
        throw EX;
    }

    @Override
    public void backgroundWait() {
        throw EX;
    }

    @Override
    public Query parseQuery(QueryContext context, IndexName name, String defaultField, String query) {
        throw EX;
    }

    @Override
    public RowType searchRowType(Session session, IndexName name) {
        throw EX;
    }


    //
    // TableListener
    //

    @Override
    public void onCreate(Session session, Table table) {
        if(!table.getFullTextIndexes().isEmpty()) {
            throw EX;
        }
    }

    @Override
    public void onDrop(Session session, Table table) {
        // None
    }

    @Override
    public void onTruncate(Session session, Table table, boolean isFast) {
        // NOne
    }

    @Override
    public void onCreateIndex(Session session, Collection<? extends Index> indexes) {
        for(Index i : indexes) {
            if(i.getIndexType() == IndexType.FULL_TEXT) {
                throw EX;
            }
        }
    }

    @Override
    public void onDropIndex(Session session, Collection<? extends Index> indexes) {
        // None
    }
}
