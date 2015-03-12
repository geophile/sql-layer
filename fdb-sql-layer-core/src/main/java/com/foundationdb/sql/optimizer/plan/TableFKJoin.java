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

import com.foundationdb.ais.model.ForeignKey;

/**
 * A Join using the Foreign Key 
 * @author TJones-Low
 *
 */
public class TableFKJoin extends BasePlanElement {
    private TableSource parent, child;
    private List<ComparisonCondition> conditions;
    private ForeignKey join;

    public TableFKJoin(TableSource parent, TableSource child,
            List<ComparisonCondition> conditions, ForeignKey join) {
        this.parent = parent;
        this.child = child;
        this.conditions = conditions;
        this.join = join;
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

    public ForeignKey getJoin() {
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
        parent = (TableSource)parent.duplicate(map);
        child = (TableSource)child.duplicate(map);
        conditions = duplicateList(conditions, map);
    }
    
}
