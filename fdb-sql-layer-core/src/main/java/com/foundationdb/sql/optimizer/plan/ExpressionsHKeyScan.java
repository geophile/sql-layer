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

import com.foundationdb.ais.model.HKey;

import java.util.ArrayList;
import java.util.List;

public class ExpressionsHKeyScan extends BaseScan implements EqualityColumnsScan
{
    private TableSource table;
    private HKey hKey;
    private List<ExpressionNode> columns;
    private List<ExpressionNode> keys;
    private List<ConditionExpression> conditions;

    public ExpressionsHKeyScan(TableSource table) {
        this.table = table;
        this.hKey = table.getTable().getTable().hKey();
    }

    public TableSource getTable() {
        return table;
    }

    public HKey getHKey() {
        return hKey;
    }

    @Override
    public TableSource getLeafMostTable() {
        return table;
    }

    @Override
    public List<ExpressionNode> getColumns() {
        return columns;
    }

    public void setColumns(List<ExpressionNode> columns) {
        this.columns = columns;
    }

    public List<ExpressionNode> getKeys() {
        return keys;
    }

    @Override
    public List<ConditionExpression> getConditions() {
        return conditions;
    }

    @Override
    public void addEqualityCondition(ConditionExpression condition, 
                                     ExpressionNode comparand) {
        if (conditions == null) {
            int ncols = hKey.nColumns();
            conditions = new ArrayList<>(ncols);
            keys = new ArrayList<>(ncols);            
        }
        conditions.add(condition);
        keys.add(comparand);
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof ExpressionRewriteVisitor) {
                visitComparands((ExpressionRewriteVisitor)v);
            }
            else if (v instanceof ExpressionVisitor) {
                visitComparands((ExpressionVisitor)v);
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
        for (int i = 0; i < keys.size(); i++) {
            keys.set(i, keys.get(i).accept(v));
        }
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
        for (ExpressionNode key : keys) {
            key.accept(v);
        }
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append('(');
        str.append(table);
        for (ExpressionNode key : keys) {
            str.append(", ");
            str.append(key);
        }
        if (getCostEstimate() != null) {
            str.append(", ");
            str.append(getCostEstimate());
        }
        str.append(")");
        return str.toString();
    }

}
