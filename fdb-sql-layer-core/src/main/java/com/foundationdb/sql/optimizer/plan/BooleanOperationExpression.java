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

/**
 * An operation on Boolean expressions.
 */
public class BooleanOperationExpression extends BaseExpression implements
        ConditionExpression {
    public static enum Operation {
        AND("and"), OR("or"), NOT("not");

        private String functionName;

        Operation(String functionName) {
            this.functionName = functionName;
        }

        public String getFunctionName() {
            return functionName;
        }
    }

    private Operation operation;
    private ConditionExpression left, right;

    public BooleanOperationExpression(Operation operation,
            ConditionExpression left, ConditionExpression right,
            DataTypeDescriptor sqlType, ValueNode sqlSource,
            TInstance type) {
        super(sqlType, sqlSource, type);
        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    public Operation getOperation() {
        return operation;
    }

    public ConditionExpression getLeft() {
        return left;
    }

    public ConditionExpression getRight() {
        return right;
    }

    @Override
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BooleanOperationExpression))
            return false;
        BooleanOperationExpression other = (BooleanOperationExpression) obj;
        return ((operation == other.operation) && left.equals(other.left) && right
                .equals(other.right));
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
            if (result != this)
                return result;
        }
        left = (ConditionExpression) left.accept(v);
        right = (ConditionExpression) right.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        if (right == null)
            return operation + " " + left;
        else
            return left + " " + operation + " " + right;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (ConditionExpression) left.duplicate(map);
        right = (ConditionExpression) right.duplicate(map);
    }

}
