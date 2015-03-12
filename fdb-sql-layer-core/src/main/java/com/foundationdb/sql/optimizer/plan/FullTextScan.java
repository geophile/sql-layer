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
package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.ais.model.FullTextIndex;

import java.util.List;

public class FullTextScan extends BaseScan
{
    private FullTextIndex index;
    private FullTextQuery query;
    private int limit;
    private TableSource indexTable;
    private List<ConditionExpression> conditions;

    public FullTextScan(FullTextIndex index, FullTextQuery query,
                        TableSource indexTable, List<ConditionExpression> conditions) {
        this.index = index;
        this.query = query;
        this.indexTable = indexTable;
        this.conditions = conditions;
    }

    public FullTextIndex getIndex() {
        return index;
    }

    public FullTextQuery getQuery() {
        return query;
    }

    public int getLimit() {
        return limit;
    }
    public void setLimit(int limit) {
        this.limit = limit;
    }

    public TableSource getIndexTable() {
        return indexTable;
    }

    public List<ConditionExpression> getConditions() {
        return conditions;
    }

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            // Don't own tables, right?
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append('(');
        str.append(indexTable.getName());
        str.append(" - ");
        str.append(query);
        if (limit > 0) {
            str.append(" LIMIT ");
            str.append(limit);
        }
        str.append(")");
        return str.toString();
    }

}
