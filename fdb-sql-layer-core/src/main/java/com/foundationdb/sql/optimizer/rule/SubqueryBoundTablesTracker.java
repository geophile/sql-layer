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
package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.*;

import java.util.*;

/** Annotate subqueries with their outer table references. */
public abstract class SubqueryBoundTablesTracker implements PlanVisitor, ExpressionVisitor {
    protected PlanContext planContext;
    protected BaseQuery rootQuery;
    protected Deque<SubqueryState> subqueries = new ArrayDeque<>();
    boolean trackingTables = true;


    protected SubqueryBoundTablesTracker(PlanContext planContext) {
        this.planContext = planContext;
    }

    protected void run() {
        rootQuery = (BaseQuery)planContext.getPlan();
        rootQuery.accept(this);
    }

    @Override
    public boolean visitEnter(PlanNode n) {
        if(n instanceof HashTableLookup)
            trackingTables = false;
        if (n instanceof Subquery) {
            subqueries.push(new SubqueryState((Subquery)n));
            return true;
        }
        return visit(n);
    }
    
    @Override
    public boolean visitLeave(PlanNode n) {
        if (n instanceof Subquery) {
            SubqueryState s = subqueries.pop();
            Set<ColumnSource> outerTables = s.getTablesReferencedButNotDefined();
            s.subquery.setOuterTables(outerTables);
            if (!subqueries.isEmpty())
                subqueries.peek().tablesReferenced.addAll(outerTables);
        }
        if(n instanceof HashTableLookup)
            trackingTables = true;
        return true;
    }

    @Override
    public boolean visit(PlanNode n) {
        if (trackingTables && !subqueries.isEmpty() &&
            (n instanceof ColumnSource)) {
            boolean added = subqueries.peek().tablesDefined.add((ColumnSource)n);
            assert added : "Table defined more than once";
        }
        return true;
    }

    @Override
    public boolean visitEnter(ExpressionNode n) {
        return visit(n);
    }

    @Override
    public boolean visitLeave(ExpressionNode n) {
        return true;
    }

    @Override
    public boolean visit(ExpressionNode n) {
        if (!subqueries.isEmpty() &&
            (n instanceof ColumnExpression)) {
            subqueries.peek().tablesReferenced.add(((ColumnExpression)n).getTable());
        }
        return true;
    }

    protected BaseQuery currentQuery() {
        if (subqueries.isEmpty()) {
            return rootQuery;
        }
        else {
            return subqueries.peek().subquery;
        }
    }

    static class SubqueryState {
        Subquery subquery;
        Set<ColumnSource> tablesReferenced = new HashSet<>();
        Set<ColumnSource> tablesDefined = new HashSet<>();

        public SubqueryState(Subquery subquery) {
            this.subquery = subquery;
        }

        public Set<ColumnSource> getTablesReferencedButNotDefined() {
            tablesReferenced.removeAll(tablesDefined);
            return tablesReferenced;
        }
    }
}
