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

/** LIMIT / OFFSET */
public class Limit extends BasePlanWithInput
{
    private int offset, limit;
    private boolean offsetIsParameter, limitIsParameter;

    public Limit(PlanNode input,
                 int offset, boolean offsetIsParameter,
                 int limit, boolean limitIsParameter) {
        super(input);
        this.offset = offset;
        this.offsetIsParameter = offsetIsParameter;
        this.limit = limit;
        this.limitIsParameter = limitIsParameter;
    }

    public Limit(PlanNode input, int limit) {
        this(input, 0, false, limit, false);
    }

    public int getOffset() {
        return offset;
    }
    public boolean isOffsetParameter() {
        return offsetIsParameter;
    }
    public int getLimit() {
        return limit;
    }
    public boolean isLimitParameter() {
        return limitIsParameter;
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(");
        if (offset > 0) {
            str.append("OFFSET ");
            if (offsetIsParameter) str.append("$");
            str.append(offset);
        }
        if (limit >= 0) {
            if (offset > 0) str.append(" ");
            str.append("LIMIT ");
            if (limitIsParameter) str.append("$");
            str.append(limit);
        }
        str.append(")");
        return str.toString();
    }

}
