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
package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Join implements HasGroup, Constraint
{
    public static Join create(AkibanInformationSchema ais,
                              String joinName,
                              Table parent,
                              Table child)
    {
        ais.checkMutability();
        Join join = new Join(joinName, parent, child);
        join.parent.addCandidateChildJoin(join);
        join.child.addCandidateParentJoin(join);
        AISInvariants.checkDuplicateConstraintsInSchema(ais, join.getConstraintName());
        ais.addJoin(join);
        ais.addConstraint(join);
        return join;
    }

    // used by the Foreign Key to track internal joins. 
    protected static Join create (String joinName, Table parent, Table child) {
        Join join = new Join (joinName, parent, child);
        return join;
    }
    
    @Override
    public String toString()
    {
        return
                getGroup() == null
                ? "Join(" + joinName + ": " + child + " -> " + parent + ")"
                : "Join(" + joinName + ": " + child + " -> " + parent + ", group(" + getGroup().getName() + "))";
    }

    public JoinColumn addJoinColumn(Column parent, Column child)
    {
        assert this.childColumns == null : "Modifying fixed Join child columns";
        assert this.parentColumns == null: "Modifying fixed Join parent columns";
        JoinColumn joinColumn = new JoinColumn(this, parent, child);
        joinColumns.add(joinColumn);
        return joinColumn;
    }

    public String getDescription()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(parent);
        buffer.append(" <- ");
        buffer.append(child);
        return buffer.toString();
    }

    public String getName()
    {
        return joinName;
    }

    public Table getParent()
    {
        return parent;
    }

    public Table getChild()
    {
        return child;
    }

    public Group getGroup()
    {
        return group;
    }

    public void setGroup(Group group)
    {
        this.group = group;
    }

    public List<JoinColumn> getJoinColumns()
    {
        return joinColumns;
    }

    public List<Column> getChildColumns() {
        if (this.childColumns == null) {
            List<Column> childColumns = new ArrayList<Column>(joinColumns.size());
            for (JoinColumn joinColumn : joinColumns) {
                childColumns.add(joinColumn.getChild());
            }
            this.childColumns =  Collections.unmodifiableList(childColumns);
        }
        return this.childColumns;
    }
    
    public List<Column> getParentColumns()  {
        if (this.parentColumns == null) {
            List<Column> parentColumns = new ArrayList<Column>(joinColumns.size());
            for (JoinColumn joinColumn : joinColumns) {
               parentColumns.add(joinColumn.getParent());
            }
            this.parentColumns = Collections.unmodifiableList(parentColumns);
        }
        return this.parentColumns;
    }
    
    public Column getMatchingChild(Column parentColumn)
    {
        for (JoinColumn joinColumn : joinColumns) {
            if (joinColumn.getParent() == parentColumn) {
                return joinColumn.getChild();
            }
        }
        return null;
    }

    public Column getMatchingParent(Column childColumn)
    {
        for (JoinColumn joinColumn : joinColumns) {
            if (joinColumn.getChild() == childColumn) {
                return joinColumn.getParent();
            }
        }
        return null;
    }

    
    public void replaceName(String newName)
    {
        joinName = newName;
    }

    @Override
    public Table getConstraintTable() {
        return child;
    }

    @Override
    public TableName getConstraintName(){
        return constraintName;
    }
    
    private Join (String joinName, Table parent, Table child) {
        this.joinName = joinName;
        this.parent = parent;
        this.child = child;
        joinColumns = new LinkedList<>();
        this.constraintName = new TableName(parent.getName().getSchemaName(), joinName);
    }
    // State

    private final Table parent;
    private final Table child;
    private final List<JoinColumn> joinColumns;
    private List<Column> childColumns;
    private List<Column> parentColumns;
    private String joinName;
    private Group group;
    private TableName constraintName;
}
