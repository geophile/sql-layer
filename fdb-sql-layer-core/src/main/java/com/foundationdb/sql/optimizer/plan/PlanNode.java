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

public interface PlanNode extends PlanElement
{
    public PlanWithInput getOutput();

    public void setOutput(PlanWithInput output);

    public boolean accept(PlanVisitor v);

    /** One-line summary of just this node.
     * @param configuration configuration options for how the plan should be printed
     */
    public String summaryString(SummaryConfiguration configuration);

    /** Hierarchical format of this node and any inputs.
     * @param configuration configuration options for how the plan should be printed
     */
    public String planString(SummaryConfiguration configuration);

    class SummaryConfiguration {
        public final boolean includeRowTypes;
        public final boolean includeIndexTableNames;

        public SummaryConfiguration(boolean includeRowTypes, boolean includeIndexTableNames) {
            this.includeRowTypes = includeRowTypes;
            this.includeIndexTableNames = includeIndexTableNames;
        }

        public static final SummaryConfiguration DEFAULT = new SummaryConfiguration(false, false);
    }
}
