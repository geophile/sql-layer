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

import com.foundationdb.ais.model.IndexName;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.LeafCursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindingsCursor;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.*;

import org.apache.lucene.search.Query;

public class IndexScan_FullText extends Operator
{
    private final IndexName index;
    private final FullTextQueryExpression queryExpression;
    private final int limit;
    private final RowType rowType;

    public IndexScan_FullText(IndexName index, 
                              FullTextQueryExpression queryExpression, 
                              int limit,
                              RowType rowType) {
        this.index = index;
        this.queryExpression = queryExpression;
        this.limit = limit;
        this.rowType = rowType;
    }

    @Override
    public RowType rowType() {
        if (rowType != null)
            return rowType;
        else
            return super.rowType(); // Only when testing and not needed.
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, bindingsCursor);
    }
    
    protected class Execution extends LeafCursor {
        private final FullTextIndexService service;
        private RowCursor cursor;

        public Execution(QueryContext context, QueryBindingsCursor bindingsCursor) {
            super(context, bindingsCursor);
            service = context.getServiceManager().getServiceByClass(FullTextIndexService.class);
            if (!queryExpression.needsBindings()) {
                // Can reuse cursor if it doesn't need bindings at open() time.
                Query query = queryExpression.getQuery(context, null);
                cursor = service.searchIndex(context, index, query, limit);
            }
        }

        @Override
        public void open()
        {
            super.open();
            if (queryExpression.needsBindings()) {
                Query query = queryExpression.getQuery(context, bindings);
                cursor = service.searchIndex(context, index, query, limit);
            }
            cursor.open();
        }

        @Override
        public Row next()
        {
            if (CURSOR_LIFECYCLE_ENABLED) {
                CursorLifecycle.checkIdleOrActive(this);
            }
            checkQueryCancelation();
            return cursor.next();
        }

        @Override
        public void jump(Row row, ColumnSelector columnSelector)
        {
            if (CURSOR_LIFECYCLE_ENABLED) {
                CursorLifecycle.checkIdleOrActive(this);
            }
            cursor.jump(row, columnSelector);
            state = CursorLifecycle.CursorState.ACTIVE;
        }

        @Override
        public void close()
        {
            try {
                if (cursor != null) {
                    cursor.close();
                    cursor = null;
                /*
                if (queryExpression.needsBindings()) {
                    cursor = null;
                }
                else {
                    cursor.close();
                }
                */
                }
            } finally {
                super.close();
            }
        }

        @Override
        public boolean isIdle()
        {
            return (cursor == null) ? super.isIdle() : cursor.isIdle();
        }

        @Override
        public boolean isActive()
        {
            return (cursor != null) && cursor.isActive();
        }

        @Override
        public boolean isClosed()
        {
            return (cursor == null) ? super.isClosed() : cursor.isClosed();
        }
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.INDEX, PrimitiveExplainer.getInstance(index.toString()));
        atts.put(Label.INDEX_KIND, PrimitiveExplainer.getInstance("FULL_TEXT"));
        atts.put(Label.PREDICATE, queryExpression.getExplainer(context));
        if (limit > 0)
            atts.put(Label.LIMIT, PrimitiveExplainer.getInstance(limit));
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        return new CompoundExplainer(Type.SCAN_OPERATOR, atts);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getName());
        str.append("(").append(index);
        str.append(" ").append(queryExpression);
        if (limit > 0) {
            str.append(" LIMIT ").append(limit);
        }
        str.append(")");
        return str.toString();
    }

}
