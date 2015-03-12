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

import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.types.TInstance;

/** An expression evaluating a column in an actual table. */
public class ColumnExpression extends BaseExpression 
{
    private ColumnSource table;
    private Column column;
    private int position;

    public ColumnExpression(TableSource table, Column column, 
                            DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource, column.getType());
        this.table = table;
        assert (table.getTable().getTable() == column.getTable());
        this.column = column;
        this.position = column.getPosition();
    }

    public ColumnExpression(ColumnExpression c, ColumnSource columnSource){
        super(c.getSQLtype(), c.getSQLsource(), c.getType());
        this.table = columnSource;
        this.position = c.getPosition();
    }
    
    public ColumnExpression(ColumnSource table, int position, 
                            DataTypeDescriptor sqlType, ValueNode sqlSource,
                            TInstance type) {
        super(sqlType, sqlSource, type);
        this.table = table;
        this.position = position;
    }

    // Generated column references without an original SQL source.
    public ColumnExpression(TableSource table, Column column) {
        this(table, column, null, null);
    }

    public ColumnSource getTable() {
        return table;
    }

    public Column getColumn() {
        return column;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ColumnExpression)) return false;
        ColumnExpression other = (ColumnExpression)obj;
        return (table.equals(other.table) &&
                ((column != null) ?
                 (column == other.column) :
                 (position == other.position)));
    }

    @Override
    public int hashCode() {
        int hash = table.hashCode();
        hash += position;
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        return v.visit(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        if (column != null)
            return table.getName() + "." + column.getName();
        else
            return table.getName() + "[" + position + "]";
    }

    @Override
    public boolean isColumn() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        table = (ColumnSource)table.duplicate(map);
    }
}
