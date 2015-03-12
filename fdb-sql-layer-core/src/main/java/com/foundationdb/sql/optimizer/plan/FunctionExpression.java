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
import com.foundationdb.server.types.TOverload;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.util.SparseArray;

import java.util.List;

/** A call to a function.
 */
public class FunctionExpression extends BaseExpression implements ResolvableExpression<TValidatedScalar>
{
    private String function;
    private List<ExpressionNode> operands;
    private TValidatedScalar overload;
    private SparseArray<Object> preptimeValues;
    private TPreptimeContext preptimeContext;

    public FunctionExpression(String function,
                              List<ExpressionNode> operands,
                              DataTypeDescriptor sqlType, ValueNode sqlSource,
                              TInstance type) {
        super(sqlType, sqlSource, type);
        this.function = function;
        this.operands = operands;
    }

    @Override
    public String getFunction() {
        return function;
    }
    public List<ExpressionNode> getOperands() {
        return operands;
    }

    @Override
    public void setResolved(TValidatedScalar resolved) {
        this.overload = resolved;
    }

    @Override
    public TValidatedScalar getResolved() {
        return overload;
    }

    public boolean anyNullTolerant() {
        if (overload == null)
            return true;        // Err on the side of caution.
        TOverload underlying = overload.getUnderlying();
        if (!(underlying instanceof TScalarBase))
            return true;
        return !((TScalarBase)underlying).allContaminatingNulls(operands.size());
    }

    public SparseArray<Object> getPreptimeValues() {
        return preptimeValues;
    }

    public void setPreptimeValues(SparseArray<Object> values) {
        this.preptimeValues = values;
    }

    @Override
    public TPreptimeContext getPreptimeContext() {
        return preptimeContext;
    }

    @Override
    public void setPreptimeContext(TPreptimeContext context) {
        this.preptimeContext = context;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FunctionExpression)) return false;
        FunctionExpression other = (FunctionExpression)obj;
        return (function.equals(other.function) &&
                operands.equals(other.operands));
    }

    @Override
    public int hashCode() {
        int hash = function.hashCode();
        hash += operands.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            for (ExpressionNode operand : operands) {
                if (!operand.accept(v))
                    break;
            }
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
        for (int i = 0; i < operands.size(); i++) {
            operands.set(i, operands.get(i).accept(v));
        }
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(function);
        str.append("(");
        boolean first = true;
        for (ExpressionNode operand : operands) {
            if (first) first = false; else str.append(",");
            str.append(operand);
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        operands = duplicateList(operands, map);
    }

}
