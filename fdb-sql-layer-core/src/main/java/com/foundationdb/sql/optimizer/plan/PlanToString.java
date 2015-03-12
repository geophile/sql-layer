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

import java.util.Deque;
import java.util.ArrayDeque;

public class PlanToString implements PlanVisitor, ExpressionVisitor
{
    public PlanToString(PlanNode.SummaryConfiguration configuration) {
        this.summaryConfiguration = configuration;
    }

    public static String of(PlanNode plan, PlanNode.SummaryConfiguration configuration) {
        PlanToString str = new PlanToString(configuration);
        str.add(plan);
        return str.toString();
    }
    
    private StringBuilder string = new StringBuilder();
    private Deque<PlanNode> pending = new ArrayDeque<>();
    private int planDepth = 0, expressionDepth = 0;
    private final PlanNode.SummaryConfiguration summaryConfiguration;


    protected void add(PlanNode n) {
        pending.addLast(n);
    }

    @Override
    public String toString() {
        while (!pending.isEmpty()) {
            PlanNode p = pending.removeFirst();
            if (string.length() > 0)
                string.append("\n");
            p.accept(this);
        }
        return string.toString();
    }

    @Override
    public boolean visitEnter(PlanNode n) {
        boolean result = visit(n);
        planDepth++;
        return result;
    }

    @Override
    public boolean visitLeave(PlanNode n) {
        planDepth--;
        return true;
    }

    @Override
    public boolean visit(PlanNode n) {
        if (expressionDepth > 0) {
            // Don't print subquery in expression until get back out to top-level.
            add(n);
            return false;
        }
        if (string.length() > 0) string.append("\n");
        for (int i = 0; i < planDepth; i++)
            string.append("  ");
        string.append(n.summaryString(summaryConfiguration));
        return true;
    }
    
    @Override
    public boolean visitEnter(ExpressionNode n) {
        boolean result = visit(n);
        expressionDepth++;
        return result;
    }

    @Override
    public boolean visitLeave(ExpressionNode n) {
        expressionDepth--;
        return true;
    }

    @Override
    public boolean visit(ExpressionNode n) {
        return true;
    }

}
