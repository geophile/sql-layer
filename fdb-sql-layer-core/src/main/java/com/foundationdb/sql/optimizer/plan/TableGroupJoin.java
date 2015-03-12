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

import com.foundationdb.ais.model.Join;

import java.util.List;

/** A join within a group corresponding to the GROUPING FK constraint. 
 */
public class TableGroupJoin extends BasePlanElement
{
    private TableGroup group;
    private TableSource parent, child;
    private List<ComparisonCondition> conditions;
    private Join join;

    public TableGroupJoin(TableGroup group,
                          TableSource parent, TableSource child,
                          List<ComparisonCondition> conditions, Join join) {
        this.group = group;
        this.parent = parent;
        parent.setGroup(group);
        this.child = child;
        this.conditions = conditions;
        for (ComparisonCondition condition : conditions)
            condition.setImplementation(ConditionExpression.Implementation.GROUP_JOIN);
        child.setParentJoin(this);
        this.join = join;
        group.addJoin(this);
    }

    public TableGroup getGroup() {
        return group;
    }
    protected void setGroup(TableGroup group) {
        this.group = group;
    }

    public TableSource getParent() {
        return parent;
    }
    public TableSource getChild() {
        return child;
    }
    public List<ComparisonCondition> getConditions() {
        return conditions;
    }
    public Join getJoin() {
        return join;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16) +
            "(" + join + ")";
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        group = (TableGroup)group.duplicate(map);
        parent = (TableSource)parent.duplicate(map);
        child = (TableSource)child.duplicate(map);
        conditions = duplicateList(conditions, map);
    }
    
    /** When a group join is across a continguous set of join, it
     * isn't part of the group any more. It is still specially marked
     * for consideration of group operators as access paths in a
     * regular join.
     */
    public void reject() {
        for (ComparisonCondition condition : conditions)
            condition.setImplementation(ConditionExpression.Implementation.POTENTIAL_GROUP_JOIN);
        child.setParentJoin(null);
        group.rejectJoin(this);
    }

}
