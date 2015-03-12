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

import java.util.List;

import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;

/** A set operation on two input streams.(UNION, INTERSECT, EXCEPT )*/
public class SetPlanNode extends BasePlanNode implements PlanWithInput, TypedPlan, ColumnSource
{
    public enum opEnum { UNION, INTERSECT, EXCEPT, NULL }

    private PlanNode left, right;
    private boolean all;
    private List<ResultField> results;
    private String opName;
    private opEnum operationType = opEnum.NULL;


    public SetPlanNode(PlanNode left, PlanNode right, boolean all, String opName) {
        this.left = left;
        left.setOutput(this);
        this.right = right;
        right.setOutput(this);
        this.all = all;
        this.opName = opName;
    }

    public PlanNode getLeft() {
        return left;
    }

    public void setLeft(PlanNode left) {
        this.left = left;
        left.setOutput(this);
    }

    public PlanNode getRight() {
        return right;
    }

    public void setRight(PlanNode right) {
        this.right = right;
        right.setOutput(this);
    }

    public boolean isAll() {
        return all;
    }

    public List<ResultField> getResults() {
        return results;
    }

    public void setResults (List<ResultField> results) {
        this.results = results;
    }

    @Override
    public int nFields() {
        return results.size();
    }

    @Override
    public TInstance getTypeAt(int index) {
        return results.get(index).getType();
    }

    @Override
    public void setTypeAt(int index, TPreptimeValue value) {
        results.get(index).setType(value.type());
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (left == oldInput) {
            left = newInput;
            left.setOutput(this);
        }
        if (right == oldInput) {
            right = newInput;
            right.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (left.accept(v))
                right.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder sb = new StringBuilder(opName);
        sb.append('@');
        sb.append(Integer.toString(hashCode(), 16));
        if (all) {
            sb.append("(ALL)");
        }
        if (configuration.includeRowTypes) {
            sb.append('[');
            for (ResultField field : results) {
                sb.append(field);
                sb.append(" (");
                sb.append(field.getType());
                sb.append("), ");
            }
            if (results.size() > 0) {
                sb.setLength(sb.length()-2);
            }
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (PlanNode)left.duplicate(map);
        right = (PlanNode)right.duplicate(map);
    }

    public String getName(){
        return opName;
    }

    public opEnum getOperationType(){
        return operationType;
    }

    public void setOperationType(opEnum operationType){
        this.operationType = operationType;
    }




}
