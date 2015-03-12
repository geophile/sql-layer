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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Take nested rows and join into single rowset. */
public class Flatten extends BasePlanWithInput
{
    // Must sometimes flatten in tables that aren't known to the
    // query, but are used as branchpoints for products.
    // This is the complete list.
    private List<TableNode> tableNodes;
    // This parallel list has nulls for those unknown tables.
    private List<TableSource> tableSources;
    // This list is one shorter and joins between each pair.
    private List<JoinType> joinTypes;

    public Flatten(PlanNode input, 
                   List<TableNode> tableNodes, 
                   List<TableSource> tableSources, 
                   List<JoinType> joinTypes) {
        super(input);
        this.tableNodes = tableNodes;
        this.tableSources = tableSources;
        this.joinTypes = joinTypes;
        assert (joinTypes.size() == tableSources.size() - 1);
    }

    public List<TableNode> getTableNodes() {
        return tableNodes;
    }

    public List<TableSource> getTableSources() {
        return tableSources;
    }

    public List<JoinType> getJoinTypes() {
        return joinTypes;
    }

    /** Get the tables involved in the sequence of inner joins, after
     * any RIGHTs and before any LEFTs. */
    public Set<TableSource> getInnerJoinedTables() {
        int rightmostRight = joinTypes.lastIndexOf(JoinType.RIGHT); // or -1
        int leftmostLeft = joinTypes.indexOf(JoinType.LEFT);
        if (leftmostLeft < 0)
            leftmostLeft = joinTypes.size();
        assert (rightmostRight < leftmostLeft);
        return new HashSet<>(tableSources.subList(rightmostRight + 1,
                                                             leftmostLeft + 1));
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(");
        for (int i = 0; i < tableNodes.size(); i++) {
            if (i > 0) {
                str.append(" ");
                str.append(joinTypes.get(i-1));
                str.append(" ");
            }
            if (tableSources.get(i) != null)
                str.append(tableSources.get(i).getName());
            else
                str.append(tableNodes.get(i));
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        tableNodes = new ArrayList<>(tableNodes);
        tableSources = duplicateList(tableSources, map);
        joinTypes = new ArrayList<>(joinTypes);
    }

}
