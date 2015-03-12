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

import com.foundationdb.server.collation.AkCollator;

import java.util.List;

/** A key expression in an ORDER BY clause. */
public class Sort extends BasePlanWithInput
{
    public static class OrderByExpression extends AnnotatedExpression {
        private boolean ascending;

        public OrderByExpression(ExpressionNode expression, boolean ascending) {
            super(expression);
            this.ascending = ascending;
        }

        public boolean isAscending() {
            return ascending;
        }
        public void setAscending(boolean ascending) {
            this.ascending = ascending;
        }

        public AkCollator getCollator() {
            return getExpression().getCollator();
        }

        public String toString() {
            if (ascending)
                return super.toString();
            else
                return super.toString() + " DESC";
        }
    }

    private List<OrderByExpression> orderBy;

    public Sort(PlanNode input, List<OrderByExpression> orderBy) {
        super(input);
        this.orderBy = orderBy;
    }

    public List<OrderByExpression> getOrderBy() {
        return orderBy;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (OrderByExpression expr : orderBy) {
                        expr.accept((ExpressionRewriteVisitor)v);
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (OrderByExpression expr : orderBy) {
                        if (!expr.getExpression().accept((ExpressionVisitor)v)) {
                            break;
                        }
                    }
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString(SummaryConfiguration configuration) {
        return super.summaryString(configuration) + orderBy;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        orderBy = duplicateList(orderBy, map);
    }

}
