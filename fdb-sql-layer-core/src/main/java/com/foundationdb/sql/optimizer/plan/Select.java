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

/** Apply a conjunction of Boolean expressions to the input.
 */
public class Select extends BasePlanWithInput
{
    private ConditionList conditions;

    public Select(PlanNode input, ConditionList conditions) {
        super(input);
        this.conditions = conditions;
    }

    public ConditionList getConditions() {
        return conditions;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    conditions.accept((ExpressionRewriteVisitor)v);
                }
                else if (v instanceof ExpressionVisitor) {
                    conditions.accept((ExpressionVisitor)v);
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString(SummaryConfiguration configuration) {
        return super.summaryString(configuration) + conditions.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        conditions = conditions.duplicate(map);
    }

}
