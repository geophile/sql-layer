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
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

/** Cast the result of expression evaluation to a given type.
 */
public class CastExpression extends BaseExpression 
{
    private ExpressionNode inner;

    public CastExpression(ExpressionNode inner, 
                          DataTypeDescriptor sqlType, ValueNode sqlSource,
                          TInstance type) {
        super(sqlType, sqlSource, type);
        this.inner = inner;
    }

    public ExpressionNode getOperand() {
        return inner;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CastExpression)) return false;
        CastExpression other = (CastExpression)obj;
        return (getSQLtype().equals(other.getSQLtype()) &&
                inner.equals(other.inner));
    }

    @Override
    public int hashCode() {
        int hash = getSQLtype().hashCode();
        hash += inner.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            inner.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        boolean childrenFirst = v.visitChildrenFirst(this);
        if (!childrenFirst) {
            ExpressionNode result = v.visit(this);
            if (result != this) return result;
        }
        inner = inner.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        Object typeDescriptor;
        TPreptimeValue tpv = getPreptimeValue();
        if (tpv != null) {
            TInstance type = tpv.type();
            typeDescriptor = type == null ? "<unknown>" : type;
        }
        else {
            typeDescriptor = getSQLtype();
        }
        return "Cast(" + inner + " AS " + typeDescriptor + ")";
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        inner = (ExpressionNode)inner.duplicate(map);
    }

}
