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

/** Track which tables a conditional expression depends on that are
 * not bound from an outer context.
 * Used to determine exactly when the condition can be tested.
 */
public class ConditionDependencyAnalyzer implements PlanVisitor, ExpressionVisitor {
    private enum State { GATHER, ANALYZE };
    private State state;
    private Set<ColumnSource> upstreamTables;
    private Set<ColumnSource> referencedTables;
    private Set<ColumnExpression> referencedColumns;

    /** Make an analyzer using the upstream tables that input to the
     * given node. */
    public ConditionDependencyAnalyzer(PlanNode node) {
        upstreamTables = new HashSet<>();
        state = State.GATHER;
        node.accept(this);
    }

    /** Make an analyzer that recognizes tables from either of two sources. */
    public ConditionDependencyAnalyzer(ConditionDependencyAnalyzer a1,
                                       ConditionDependencyAnalyzer a2) {
        upstreamTables = new HashSet<>(a1.upstreamTables);
        upstreamTables.addAll(a2.upstreamTables);
    }

    /** Get the tables that are not bound by some outer contour and so
     * must be available at the point of execution. */
    public Set<ColumnSource> getUpstreamTables() {
        return upstreamTables;
    }

    /** Analyze the given condition and if it acccesses just a single
     * table, return that.  Otherwise, return <code>null</code>.
     */
    public ColumnSource analyze(ConditionExpression cond) {
        referencedTables = new HashSet<>();
        referencedColumns = new HashSet<>();
        state = State.ANALYZE;
        cond.accept(this);
        if (referencedTables.size() == 1)
            return referencedTables.iterator().next();
        else
            return null;
    }

    /** Get the tables that were referenced by the condition. */
    public Set<ColumnSource> getReferencedTables() {
        return referencedTables;
    }

    /** Get the columns that were referenced by the condition. */
    public Set<ColumnExpression> getReferencedColumns() {
        return referencedColumns;
    }

    @Override
    public boolean visitEnter(PlanNode n) {
        return visit(n);
    }

    @Override
    public boolean visitLeave(PlanNode n) {
        return true;
    }

    @Override
    public boolean visit(PlanNode n) {
        if (state == State.GATHER) {
            if (n instanceof ColumnSource)
                upstreamTables.add((ColumnSource)n);
            else if (n instanceof IndexScan)
                upstreamTables.addAll(((IndexScan)n).getTables());
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
        if (state == State.ANALYZE) {
            if (n instanceof ColumnExpression) {
                ColumnExpression column = (ColumnExpression)n;
                ColumnSource table = column.getTable();
                if (upstreamTables.contains(table)) {
                    referencedTables.add(table);
                    referencedColumns.add(column);
                }
            }
        }
        return true;
    }
}
