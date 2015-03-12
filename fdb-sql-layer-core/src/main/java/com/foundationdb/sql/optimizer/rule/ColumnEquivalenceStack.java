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

import com.foundationdb.sql.optimizer.plan.BaseQuery;
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.PlanNode;

import java.util.ArrayDeque;
import java.util.Deque;

public final class ColumnEquivalenceStack {
    private static final int EQUIVS_DEQUE_SIZE = 3;
    private Deque<EquivalenceFinder<ColumnExpression>> stack
            = new ArrayDeque<>(EQUIVS_DEQUE_SIZE);
    private Deque<EquivalenceFinder<ColumnExpression>> fkStack =
            new ArrayDeque<>(EQUIVS_DEQUE_SIZE);
            
    public boolean enterNode(PlanNode n) {
        if (n instanceof BaseQuery) {
            BaseQuery bq = (BaseQuery) n;
            stack.push(bq.getColumnEquivalencies());
            fkStack.push(bq.getFKEquivalencies());
            return true;
        }
        return false;
    }
    
    public EquivalenceFinder<ColumnExpression> leaveNode(PlanNode n) {
        if (n instanceof BaseQuery) {
            fkStack.removeFirst();
            return stack.pop();
        }
        return null;
    }
    
    public EquivalenceFinder<ColumnExpression> get() {
        return stack.element();
    }
    
    public EquivalenceFinder<ColumnExpression> getFks() {
        return fkStack.element();
    }
    

    public boolean isEmpty() {
        return stack.isEmpty();
    }
}
