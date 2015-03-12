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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.FullTextQueryParseException;
import com.foundationdb.server.service.session.Session;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public abstract class FullTextIndexInfosImpl implements FullTextIndexInfos
{
    protected final Map<IndexName,FullTextIndexShared> indexes = new HashMap<>();
    
    @Override
    public Query parseQuery(QueryContext context, IndexName name, 
                            String defaultField, String query) {
        FullTextIndexInfo index = getIndex(context.getSession(), name, null);
        if (defaultField == null) {
            defaultField = index.getDefaultFieldName();
        }
        StandardQueryParser parser = index.getParser();
        try {
            synchronized (parser) {
                return parser.parse(query, defaultField);
            }
        }
        catch (QueryNodeException ex) {
            throw new FullTextQueryParseException(ex);
        }
    }

    @Override
    public RowType searchRowType(Session session, IndexName name) {
        FullTextIndexInfo index = getIndex(session, name, null);
        return index.getHKeyRowType();
    }

    protected abstract AkibanInformationSchema getAIS(Session session);
    protected abstract File getIndexPath();

    protected FullTextIndexInfo getIndexIfExists(Session session, IndexName name, AkibanInformationSchema ais) {
        if (ais == null)
            ais = getAIS(session);
        FullTextIndexInfo info = null;
        synchronized (indexes) {
            FullTextIndexShared shared = indexes.get(name);
            if (shared != null) {
                info = shared.forAIS(ais);
            }
        }
        return info;
    }

    protected FullTextIndexInfo getIndex(Session session, IndexName name, AkibanInformationSchema ais) {
        if (ais == null)
            ais = getAIS(session);
        FullTextIndexInfo info;
        synchronized (indexes) {
            FullTextIndexShared shared = indexes.get(name);
            if (shared != null) {
                info = shared.forAIS(ais);
            }
            else {
                shared = new FullTextIndexShared(name);
                info = new FullTextIndexInfo(shared);
                info.init(ais);
                info = shared.init(ais, info, getIndexPath());
                indexes.put(name, shared);
            }
        }
        return info;
    }
}
