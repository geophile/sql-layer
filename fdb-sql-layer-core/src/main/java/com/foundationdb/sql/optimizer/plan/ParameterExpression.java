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

import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

/** An operand with a parameter value. */
public class ParameterExpression extends BaseExpression 
{
    private int position;
    private Object value;
    private boolean isSet;

    public ParameterExpression(int position, 
                               DataTypeDescriptor sqlType, ValueNode sqlSource,
                               TInstance type) {
        super(sqlType, sqlSource, type);
        this.position = position;
        this.value = null;
        this.isSet = false;
    }

    public ParameterExpression(int position, 
            DataTypeDescriptor sqlType, ValueNode sqlSource,
            TInstance type, Object value) {
        super(sqlType, sqlSource, type);
        this.position = position;
        this.value = value;
        this.isSet = true;
    }

    
    public int getPosition() {
        return position;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ParameterExpression)) return false;
        ParameterExpression other = (ParameterExpression)obj;
        return (position == other.position);
    }

    @Override
    public int hashCode() {
        return position;
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
        return "$" + position;
    }

    public void setValue(Object value) {
        this.value = value;
        isSet = true;
    }

    public Object getValue() {
        return value;
    }

    public boolean isSet() {
        return isSet;
    }
}
