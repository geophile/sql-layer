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

import java.util.List;

/** This is what IN gets turned into late in the game after
 * optimizations have been exhausted. */
public class InListCondition extends BaseExpression implements ConditionExpression 
{
    private ExpressionNode operand;
    private List<ExpressionNode> expressions;
    private ComparisonCondition comparison;

    public InListCondition(ExpressionNode operand, List<ExpressionNode> expressions,
                           DataTypeDescriptor sqlType, ValueNode sqlSource,
                           TInstance type) {
        super(sqlType, sqlSource, type);
        this.operand = operand;
        this.expressions = expressions;
    }

    public ExpressionNode getOperand() {
        return operand;
    }
    public void setOperand(ExpressionNode operand) {
        this.operand = operand;
    }

    public List<ExpressionNode> getExpressions() {
        return expressions;
    }
    public void setExpressions(List<ExpressionNode> expressions) {
        this.expressions = expressions;
    }

    public ComparisonCondition getComparison() {
        return comparison;
    }
    public void setComparison(ComparisonCondition comparison) {
        this.comparison = comparison;
    }

    @Override
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof InListCondition)) return false;
        InListCondition other = (InListCondition)obj;
        return (operand.equals(other.operand) &&
                expressions.equals(other.expressions));
    }

    @Override
    public int hashCode() {
        int hash = operand.hashCode();
        hash += expressions.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (operand.accept(v)) {
                for (ExpressionNode expression : expressions) {
                    if (!expression.accept(v))
                        break;
                }
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
        operand = operand.accept(v);
        for (int i = 0; i < expressions.size(); i++)
            expressions.set(i, expressions.get(i).accept(v));
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        return "IN(" + operand + ", " + expressions + ")";
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        operand = (ExpressionNode)operand.duplicate();
        expressions = duplicateList(expressions, map);
    }

}
