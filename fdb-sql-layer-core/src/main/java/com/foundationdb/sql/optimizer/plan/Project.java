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

import java.util.List;

/** An list of expressions making up new rows. */
public class Project extends BasePlanWithInput implements ColumnSource, TypedPlan
{
    private List<ExpressionNode> fields;

    public Project(PlanNode input, List<ExpressionNode> fields) {
        super(input);
        this.fields = fields;
    }

    public List<ExpressionNode> getFields() {
        return fields;
    }

    @Override
    public String getName() {
        return "PROJECT";
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (int i = 0; i < fields.size(); i++) {
                        fields.set(i, fields.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (ExpressionNode field : fields) {
                        if (!field.accept((ExpressionVisitor)v))
                            break;
                    }
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder stringBuilder = new StringBuilder(super.summaryString(configuration));
        if (configuration.includeRowTypes) {
            stringBuilder.append('[');
            for (ExpressionNode field : fields) {
                stringBuilder.append(field);
                stringBuilder.append(" (");
                stringBuilder.append(field.getType());
                stringBuilder.append("), ");
            }
            if (fields.size() > 0) {
                stringBuilder.setLength(stringBuilder.length()-2);
            }
            stringBuilder.append(']');
        } else {
            stringBuilder.append(fields);
        }
        return stringBuilder.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        fields = duplicateList(fields, map);
    }

    @Override
    public int nFields() {
        return fields.size();
    }

    @Override
    public TInstance getTypeAt(int index) {
        ExpressionNode field = fields.get(index);
        TPreptimeValue tpv = field.getPreptimeValue();
        return tpv.type();
    }

    @Override
    public void setTypeAt(int index, TPreptimeValue value) {
        fields.get(index).setPreptimeValue(value);
    }
}
