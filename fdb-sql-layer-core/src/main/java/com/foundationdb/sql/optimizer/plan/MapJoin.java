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

import com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;

/** A join implementation using Map. */
public class MapJoin extends BasePlanNode implements PlanWithInput
{
    // This is non-null only until the map has been folded.
    private JoinType joinType;
    private PlanNode outer, inner;

    public MapJoin(JoinType joinType, PlanNode outer, PlanNode inner) {
        this.joinType = joinType;
        this.outer = outer;
        outer.setOutput(this);
        this.inner = inner;
        inner.setOutput(this);
    }

    public JoinType getJoinType() {
        return joinType;
    }
    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public PlanNode getOuter() {
        return outer;
    }
    public void setOuter(PlanNode outer) {
        this.outer = outer;
        outer.setOutput(this);
    }
    public PlanNode getInner() {
        return inner;
    }
    public void setInner(PlanNode inner) {
        this.inner = inner;
        inner.setOutput(this);
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (outer == oldInput) {
            outer = newInput;
            outer.setOutput(this);
        }
        if (inner == oldInput) {
            inner = newInput;
            inner.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (outer.accept(v))
                inner.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(");
        if (joinType != null) {
            str.append(joinType);
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        outer = (PlanNode)outer.duplicate(map);
        inner = (PlanNode)inner.duplicate(map);
    }

}
