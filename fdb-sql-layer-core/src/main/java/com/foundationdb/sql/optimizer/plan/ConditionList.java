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

import java.util.ArrayList;
import java.util.Collection;

/** A conjunction of boolean conditions used for WHERE / HAVING / ON / ...
 */
public class ConditionList extends ArrayList<ConditionExpression>
{
    public ConditionList() {
        super();
    }

    public ConditionList(int size) {
        super(size);
    }

    public ConditionList(Collection<? extends ConditionExpression> list) {
        super(list);
    }

    public boolean accept(ExpressionVisitor v) {
        for (ConditionExpression condition : this) {
            if (!condition.accept(v)) {
                return false;
            }
        }
        return true;
    }

    public void accept(ExpressionRewriteVisitor v) {
        for (int i = 0; i < size(); i++) {
            set(i, (ConditionExpression)get(i).accept(v));
        }
    }

    public ConditionList duplicate(DuplicateMap map) {
        ConditionList copy = new ConditionList(size());
        for (ConditionExpression cond : this) {
            copy.add((ConditionExpression)cond.duplicate(map));
        }
        return copy;
    }

}
