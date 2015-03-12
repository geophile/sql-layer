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

import java.util.Collection;
import java.util.List;

public class GroupScan extends BaseScan implements TableLoader
{
    private TableGroup group;
    private List<TableSource> tables;

    public GroupScan(TableGroup group) {
        this.group = group;
    }

    public TableGroup getGroup() {
        return group;
    }

    /** The tables that this branch lookup introduces into the stream. */
    public List<TableSource> getTables() {
        return tables;
    }

    public void setTables(List<TableSource> tables) {
        this.tables = tables;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (tables != null) {
                for (TableSource table : tables) {
                    if (!table.accept(v))
                        break;
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        group = (TableGroup)group.duplicate();
        tables = duplicateList(tables, map);
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append('(');
        str.append(group.getGroup());
        if (getCostEstimate() != null) {
            str.append(", ");
            str.append(getCostEstimate());
        }
        str.append(")");
        return str.toString();
    }

    @Override
    public Collection<? extends ConditionExpression> getConditions() {
        return null;
    }

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
    }
}
