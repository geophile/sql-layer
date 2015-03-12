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

import java.util.ArrayList;
import java.util.List;

public class LogicalFunctionCondition extends FunctionCondition
{
    // TODO: Can't use operands directly without making
    // FunctionExpression generic in <T extends ExpressionNode> for
    // its operands, because there's no other way to make accept
    // generic to indicate returning the type of this.
    public LogicalFunctionCondition(String function,
                                    List<ConditionExpression> operands,
                                    DataTypeDescriptor sqlType, ValueNode sqlSource,
                                    TInstance type) {
        super(function, new ArrayList<ExpressionNode>(operands), sqlType, sqlSource, type);
    }

    public ConditionExpression getOperand() {
        assert (getOperands().size() == 1);
        return (ConditionExpression)getOperands().get(0);
    }

    public ConditionExpression getLeft() {
        assert (getOperands().size() == 2);
        return (ConditionExpression)getOperands().get(0);
    }

    public ConditionExpression getRight() {
        assert (getOperands().size() == 2);
        return (ConditionExpression)getOperands().get(1);
    }

}
