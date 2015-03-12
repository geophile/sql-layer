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

import java.util.List;

/** A product type join among several subplans. */
public class Product extends BasePlanNode implements PlanWithInput
{
    private TableNode ancestor;
    private List<PlanNode> subplans;

    public Product(TableNode ancestor, List<PlanNode> subplans) {
        this.ancestor = ancestor;
        this.subplans = subplans;
        for (PlanNode subplan : subplans)
          subplan.setOutput(this);
    }

    public TableNode getAncestor() {
        return ancestor;
    }

    public List<PlanNode> getSubplans() {
        return subplans;
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        int index = subplans.indexOf(oldInput);
        if (index >= 0) {
            subplans.set(index, newInput);
            newInput.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            for (PlanNode subplan : subplans) {
                if (!subplan.accept(v))
                    break;
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        if (ancestor != null) {
            str.append("(").append(ancestor).append(")");
        }
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        subplans = duplicateList(subplans, map);
    }

}
