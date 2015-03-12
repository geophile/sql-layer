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

import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.util.Strings;
import com.foundationdb.sql.optimizer.plan.Sort.OrderByExpression;

import java.util.List;

public final class MultiIndexIntersectScan extends IndexScan {
    
    private IndexScan outputScan;
    private IndexScan selectorScan;
    private int comparisonColumns;
    private List<ConditionExpression> conditions;

    public MultiIndexIntersectScan(IndexScan outerScan, IndexScan selectorScan, int comparisonColumns)
    {
        this(outerScan.getRootMostTable(), outerScan.getLeafMostTable());
        assert outerScan.getRootMostTable() == outerScan.getRootMostInnerTable() : outerScan;
        assert outerScan.getLeafMostTable() == outerScan.getLeafMostInnerTable() : outerScan;
        this.outputScan = outerScan;
        this.selectorScan = selectorScan;
        this.comparisonColumns = comparisonColumns;
    }
    
    private MultiIndexIntersectScan(TableSource rootMost, TableSource leafMost) {
        super(rootMost, rootMost, leafMost, leafMost);
    }
    
    public IndexScan getOutputIndexScan() {
        return outputScan;
    }
    
    public IndexScan getSelectorIndexScan() {
        return selectorScan;
    }

    public int getComparisonFields() {
        return comparisonColumns;
    }

    public boolean[] getComparisonFieldDirections() {
        boolean[] ascending = new boolean[comparisonColumns];
        for (int i = 0; i < comparisonColumns; i++) {
            ascending[i] = getOrdering().get(getPeggedCount() + i).isAscending();
        }
        return ascending;
    }

    public int getOutputOrderingFields() {
        return getOrderingFields(outputScan);
    }

    public int getSelectorOrderingFields() {
        return getOrderingFields(selectorScan);
    }

    private int getOrderingFields(IndexScan scan) {
        return scan.getAllColumns().size() - scan.getPeggedCount();
    }

    @Override
    public List<OrderByExpression> getOrdering() {
        return outputScan.getOrdering();
    }

    @Override
    public OrderEffectiveness getOrderEffectiveness() {
        return outputScan.getOrderEffectiveness();
    }

    @Override
    public List<ExpressionNode> getEqualityComparands() {
        return outputScan.getEqualityComparands();
    }

    @Override
    public ExpressionNode getLowComparand() {
        return outputScan.getLowComparand();
    }

    @Override
    public boolean isLowInclusive() {
        return outputScan.isLowInclusive();
    }

    @Override
    public ExpressionNode getHighComparand() {
        return outputScan.getHighComparand();
    }

    @Override
    public boolean isHighInclusive() {
        return outputScan.isHighInclusive();
    }

    @Override
    public boolean isSpatial() {
        return outputScan.isSpatial();
    }

    @Override
    public List<ConditionExpression> getConditions() {
        return conditions;
    }

    @Override
    public boolean hasConditions() {
        return !conditions.isEmpty();
    }

    public void setConditions(List<ConditionExpression> conditions) {
        this.conditions = conditions;
    }

    @Override
    public List<ExpressionNode> getColumns() {
        return outputScan.getColumns();
    }

    @Override
    public Table getLeafMostAisTable() {
        return outputScan.getLeafMostTable().getTable().getTable();
    }

    @Override
    public List<IndexColumn> getAllColumns() {
        return outputScan.getAllColumns();
    }

    @Override
    public int getNEquality() {
        return outputScan.getNEquality();
    }

    @Override
    public int getNUnions() {
        return outputScan.getNUnions();
    }

    @Override
    public void setIncludeUnionAsEquality(boolean sortColumn) {
        outputScan.setIncludeUnionAsEquality(sortColumn);
    }

    @Override
    public void incrementConditionsCounter(ConditionsCounter<? super ConditionExpression> counter) {
        outputScan.incrementConditionsCounter(counter);
        selectorScan.incrementConditionsCounter(counter);
    }

    @Override
    public boolean isUseful(ConditionsCount<? super ConditionExpression> count) {
        return outputScan.isUseful(count) && selectorScan.isUseful(count);
    }

    @Override
    public List<IndexColumn> getIndexColumns() {
        return outputScan.getIndexColumns();
    }

    @Override
    public int getNKeyColumns() {
        return outputScan.getNKeyColumns();
    }

    @Override
    public boolean usesAllColumns() {
        return outputScan.usesAllColumns();
    }

    @Override
    public void setUsesAllColumns(boolean usesAllColumns) {
        outputScan.setUsesAllColumns(usesAllColumns);
    }

    @Override
    protected String summarizeIndex(int indentation, SummaryConfiguration configuration) {
        boolean pretty = indentation >= 0;
        int nextIndentation = pretty ? indentation + 1 : -1;

        StringBuilder sb = new StringBuilder();
        if (pretty) {
            sb.append("compare ").append(getComparisonFields()).append(Strings.NL);
            indent(sb, nextIndentation);
        }
        else {
            sb.append("INTERSECT(compare ").append(getComparisonFields()).append(", ");
        }
        summarizeChildIndex(outputScan, nextIndentation, sb, configuration);
        if (pretty) {
            sb.append(Strings.NL);
            indent(sb, nextIndentation);
        }
        else {
            sb.append(" AND ");
        }
        summarizeChildIndex(selectorScan, nextIndentation, sb, configuration);
        if (!pretty)
            sb.append(')');
        return sb.toString();
    }

    private void summarizeChildIndex(IndexScan child, int indentation, StringBuilder sb,
                                     SummaryConfiguration configuration) {
        int skips = child.getAllColumns().size() - getOrderingFields(child);
        sb.append("skip ").append(skips).append(": ");
        child.buildSummaryString(sb, indentation, false, configuration);
    }

    @Override
    public boolean isAscendingAt(int i) {
        return outputScan.isAscendingAt(i);
    }

    @Override
    public boolean isRecoverableAt(int i) {
        return outputScan.isRecoverableAt(i);
    }

    @Override
    public Table findCommonAncestor(IndexScan other) {
        return outputScan.findCommonAncestor(other);
    }

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
        outputScan.visitComparands(v);
        selectorScan.visitComparands(v);
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
        outputScan.visitComparands(v);
        selectorScan.visitComparands(v);
    }

}
