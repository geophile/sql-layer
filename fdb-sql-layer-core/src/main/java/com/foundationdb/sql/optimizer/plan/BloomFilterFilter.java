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

/** Application of a Bloom filter. */
public class BloomFilterFilter extends BasePlanWithInput
{
    private BloomFilter bloomFilter;
    private List<ExpressionNode> lookupExpressions;
    private PlanNode check;

    public BloomFilterFilter(BloomFilter bloomFilter, List<ExpressionNode> lookupExpressions, 
                             PlanNode input, PlanNode check) {
        super(input);
        this.bloomFilter = bloomFilter;
        this.lookupExpressions = lookupExpressions;
        this.check = check;
        check.setOutput(this);
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }
    public List<ExpressionNode> getLookupExpressions() {
        return lookupExpressions;
    }

    public PlanNode getCheck() {
        return check;
    }
    public void setCheck(PlanNode check) {
        this.check = check;
        check.setOutput(this);
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        super.replaceInput(oldInput, newInput);
        if (check == oldInput) {
            check = newInput;
            check.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v) && check.accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (int i = 0; i < lookupExpressions.size(); i++) {
                        lookupExpressions.set(i, lookupExpressions.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (ExpressionNode expr : lookupExpressions) {
                        if (!expr.accept((ExpressionVisitor)v))
                            break;
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
        str.append(bloomFilter);
        str.append(", ");
        str.append(lookupExpressions);
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        check = (PlanNode)check.duplicate(map);
    }

}
