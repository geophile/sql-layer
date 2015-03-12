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

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/** A join node explicitly enumerating rows.
 * From VALUES or IN.
 */
public class ExpressionsSource extends BaseJoinable implements ColumnSource, TypedPlan
{
    private List<List<ExpressionNode>> expressions;
    private TInstance[] tInstances;

    public ExpressionsSource(List<List<ExpressionNode>> expressions) {
        this.expressions = expressions;
    }

    public List<List<ExpressionNode>> getExpressions() {
        return expressions;
    }

    public List<TPreptimeValue> getPreptimeValues() {
        if (expressions.isEmpty())
            return Collections.emptyList();
        List<ExpressionNode> nodes = expressions.get(0);
        List<TPreptimeValue> result = new ArrayList<>(nodes.size());
        for (ExpressionNode node : nodes) {
            result.add(node.getPreptimeValue());
        }
        return result;
    }

    public void setTInstances(TInstance[] tInstances) {
        this.tInstances = tInstances;
    }

    public TInstance[] getFieldTInstances() {
        return tInstances;
    }

    // TODO: It might be interesting to note when it's also sorted for
    // WHERE x IN (...) ORDER BY x.
    public static enum DistinctState {
        DISTINCT, DISTINCT_WITH_NULL, HAS_PARAMETERS, HAS_EXPRESSSIONS,
        NEED_DISTINCT
    }

    private DistinctState distinctState;

    public DistinctState getDistinctState() {
        return distinctState;
    }
    public void setDistinctState(DistinctState distinctState) {
        this.distinctState = distinctState;
    }

    @Override
    public String getName() {
        return "VALUES";
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof ExpressionRewriteVisitor) {
                for (List<ExpressionNode> row : expressions) {
                    for (int i = 0; i < row.size(); i++) {
                        row.set(i, row.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }                
            }
            else if (v instanceof ExpressionVisitor) {
                expressions:
                for (List<ExpressionNode> row : expressions) {
                    for (ExpressionNode expr : row) {
                        if (!expr.accept((ExpressionVisitor)v)) {
                            break expressions;
                        }
                    }
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(");
        str.append(expressions);
        if (distinctState != null) {
            switch (distinctState) {
            case NEED_DISTINCT:
                str.append(", ");
                str.append(distinctState);
                break;
            }
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        expressions = new ArrayList<>(expressions);
        for (int i = 0; i < expressions.size(); i++) {
            expressions.set(i, duplicateList(expressions.get(i), map));
        }
    }

    @Override
    public int nFields() {
        return expressions.get(0).size();
    }

    @Override
    public TInstance getTypeAt(int index) {
        return tInstances[index];
    }

    @Override
    public void setTypeAt(int index, TPreptimeValue value) {
        tInstances[index] = value.type();
    }
}
