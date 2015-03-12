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

import com.foundationdb.ais.model.Group;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A set of tables with common group joins.
 * These joins need not be contiguous, but group join operators can
 * still be used for access among them.
 */
public class TableGroup extends BasePlanElement
{
    private Group group;
    private Set<TableSource> tables;
    private List<TableGroupJoin> joins, rejectedJoins;

    public TableGroup(Group group) {
        this.group = group;
        tables = new HashSet<>();
        joins = new ArrayList<>();
    }

    public Group getGroup() {
        return group;
    }

    public Set<TableSource> getTables() {
        return tables;
    }

    public List<TableGroupJoin> getJoins() {
        return joins;
    }

    public void addJoin(TableGroupJoin join) {
        joins.add(join);
        tables.add(join.getParent());
        tables.add(join.getChild());
    }

    public List<TableGroupJoin> getRejectedJoins() {
        return rejectedJoins;
    }

    public void rejectJoin(TableGroupJoin join) {
        joins.remove(join);
        if (rejectedJoins == null)
            rejectedJoins = new ArrayList<>();
        rejectedJoins.add(join);
    }

    public void merge(TableGroup other) {
        assert (group == other.group);
        for (TableGroupJoin join : other.joins) {
            join.setGroup(this);
            join.getParent().setGroup(this);
            join.getChild().setGroup(this);
            addJoin(join);
        }
    }

    public int getMinOrdinal() {
        int min = Integer.MAX_VALUE;
        for (TableSource table : tables) {
            int ordinal = table.getTable().getOrdinal();
            if (min > ordinal)
                min = ordinal;
        }
        return min;
    }

    public TableSource findByOrdinal(int ordinal) {
        for (TableSource table : tables) {
            if (ordinal == table.getTable().getOrdinal()) {
                return table;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16) +
            "(" + group.getName().getTableName() + ")";
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        tables = duplicateSet(tables, map);
        joins = duplicateList(joins, map);
        if (rejectedJoins != null)
            rejectedJoins = duplicateList(rejectedJoins, map);
    }

}
