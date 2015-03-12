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

public class BranchLookup extends BaseLookup
{
    private TableNode source, ancestor, branch;

    public BranchLookup(PlanNode input, 
                        TableNode source, TableNode ancestor, TableNode branch,
                        List<TableSource> tables) {
        super(input, tables);
        this.source = source;
        this.ancestor = ancestor;
        this.branch = branch;
    }

    /** Lookup a branch right right beneath a starting point. */
    public BranchLookup(PlanNode input, TableNode source, List<TableSource> tables) {
        this(input, source, source, source, tables);
    }

    /** Lookup an immediate child of the starting point. */
    public BranchLookup(PlanNode input, TableNode source, TableNode branch, 
                        List<TableSource> tables) {
        this(input, source, source, branch, tables);
        assert(source == branch.getParent());
    }

    public TableNode getSource() {
        return source;
    }

    public TableNode getAncestor() {
        return ancestor;
    }

    public TableNode getBranch() {
        return branch;
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(").append(source).append(" -> ").append(branch);
        if (ancestor != source)
            str.append(" via ").append(ancestor);
        str.append(")");
        return str.toString();
    }

}
