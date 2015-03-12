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

import static com.foundationdb.server.service.text.FullTextQueryBuilder.BooleanType;

import java.util.ArrayList;
import java.util.List;

public class FullTextBoolean extends FullTextQuery
{
    private List<FullTextQuery> operands;
    private List<BooleanType> types;

    public FullTextBoolean(List<FullTextQuery> operands, List<BooleanType> types) {
        this.operands = operands;
        this.types = types;
    }

    public List<FullTextQuery> getOperands() {
        return operands;
    }
    public List<BooleanType> getTypes() {
        return types;
    }

    public boolean accept(ExpressionVisitor v) {
        for (FullTextQuery operand : operands) {
            if (!operand.accept(v)) {
                return false;
            }
        }
        return true;
    }

    public void accept(ExpressionRewriteVisitor v) {
        for (FullTextQuery operand : operands) {
            operand.accept(v);
        }
    }

    public FullTextBoolean duplicate(DuplicateMap map) {
        List<FullTextQuery> newOperands = new ArrayList<>(operands.size());
        for (FullTextQuery operand : operands) {
            newOperands.add((FullTextQuery)operand.duplicate(map));
        }
        return new FullTextBoolean(newOperands, new ArrayList<>(types));
    }
    
    
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("[");
        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                str.append(", ");
            }
            str.append(types.get(i));
            str.append("(");
            str.append(operands.get(i));
            str.append(")");
        }
        str.append("]");
        return str.toString();
    }

}
