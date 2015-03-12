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
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

/** A binary comparison (equality / inequality) between two expressions.
 */
public class ComparisonCondition extends BaseExpression implements ConditionExpression 
{
    private Comparison operation;
    private ExpressionNode left, right;
    private Implementation implementation;
    private TKeyComparable keyComparable;

    public ComparisonCondition(Comparison operation,
                               ExpressionNode left, ExpressionNode right,
                               DataTypeDescriptor sqlType, ValueNode sqlSource,
                               TInstance type) {
        super(sqlType, sqlSource, type);
        this.operation = operation;
        this.left = left;
        this.right = right;
        this.implementation = Implementation.NORMAL;
    }

    public Comparison getOperation() {
        return operation;
    }
    public void setComparison(Comparison operation) {
        this.operation = operation;
    }

    public ExpressionNode getLeft() {
        return left;
    }
    public void setLeft(ExpressionNode left) {
        this.left = left;
    }

    public ExpressionNode getRight() {
        return right;
    }
    public void setRight(ExpressionNode right) {
        this.right = right;
    }

    public TKeyComparable getKeyComparable() {
        return keyComparable;
    }

    public void setKeyComparable(TKeyComparable keyComparable) {
        this.keyComparable = keyComparable;
    }

    @Override
    public Implementation getImplementation() {
        return implementation;
    }
    public void setImplementation(Implementation implementation) {
        this.implementation = implementation;
    }

    public static Comparison reverseComparison(Comparison operation) {
        switch (operation) {
        case EQ:
        case NE:
            return operation;
        case LT:
            return Comparison.GT;
        case LE:
            return Comparison.GE;
        case GT:
            return Comparison.LT;
        case GE:
            return Comparison.LE;
        default:
            assert false : operation;
            return null;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ComparisonCondition)) return false;
        ComparisonCondition other = (ComparisonCondition)obj;
        return ((operation == other.operation) &&
                left.equals(other.left) &&
                right.equals(other.right));
    }

    @Override
    public int hashCode() {
        int hash = operation.hashCode();
        hash += left.hashCode();
        hash += right.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (left.accept(v))
                right.accept(v);
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
        left = left.accept(v);
        right = right.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        return left + " " + operation + " " + right;
    }

    public void reverse() {
        ExpressionNode temp = left;
        left = right;
        right = temp;
        operation = reverseComparison(operation);
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        // Index and join are likely to be pointed to elsewhere.
        return (implementation != Implementation.NORMAL);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (ExpressionNode)left.duplicate();
        right = (ExpressionNode)right.duplicate();
    }

}
