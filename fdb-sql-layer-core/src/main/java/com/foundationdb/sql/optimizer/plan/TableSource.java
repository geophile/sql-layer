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

/** A join to an actual table. */
public class TableSource extends BaseJoinable implements ColumnSource
{
    private TableNode table;
    private TableGroup group;
    private TableGroupJoin parentJoin;
    private TableFKJoin parentFKJoin;
    private boolean required;
    private String name;

    public TableSource(TableNode table, boolean required, String name)
    {
        this.table = table;
        table.addUse(this);
        this.required = required;
        this.name = name;
    }

    public TableNode getTable() {
        return table;
    }

    public TableGroup getGroup() {
        return group;
    }
    public void setGroup(TableGroup group) {
        this.group = group;
        group.getTables().add(this);
    }

    public TableGroupJoin getParentJoin() {
        return parentJoin;
    }
    public void setParentJoin(TableGroupJoin parentJoin) {
        this.parentJoin = parentJoin;
        if (parentJoin != null)
            this.group = parentJoin.getGroup();
    }
    
    public void setParentFKJoin (TableFKJoin parentFKJoin) {
        this.parentFKJoin = parentFKJoin;
    }
    
    public TableFKJoin getParentFKJoin () {
        return this.parentFKJoin;
    }

    public TableSource getParentTable() {
        if (parentJoin != null) 
            return parentJoin.getParent();
        else if (parentFKJoin != null) 
            return parentFKJoin.getParent();
        else
            return null;
    }

    public boolean isRequired() {
        return required;
    }
    public boolean isOptional() {
        return !required;
    }
    public void setRequired(boolean required) {
        this.required = required;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isTable() {
        return true;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }
    
    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(");
        str.append(name);
        if (parentJoin != null) {
            str.append(" - ");
            str.append(parentJoin);
        }
        else if (parentFKJoin != null) {
            str.append (" - ");
            str.append(parentFKJoin);
        }
        else if (group != null) {
            str.append(" - ");
            str.append(group);
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        table = map.duplicate(table);
        table.addUse(this);
    }

}
