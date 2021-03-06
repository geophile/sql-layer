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
import com.foundationdb.sql.optimizer.plan.Sort.OrderByExpression;
import com.foundationdb.util.Strings;
import java.util.*;

public abstract class IndexScan extends BaseScan implements IndexIntersectionNode<ConditionExpression,IndexScan>, JoinTreeScan
{

    public static enum OrderEffectiveness {
        NONE, PARTIAL_GROUPED, GROUPED, SORTED, FOR_MIN_MAX
    }

    private TableSource rootMostTable, rootMostInnerTable, leafMostInnerTable, leafMostTable;

    private boolean covering;

    // The cost of just the scan of this index, not counting lookups, flattening, etc
    private CostEstimate scanCostEstimate;

    public IndexScan(TableSource table) {
        rootMostTable = rootMostInnerTable = leafMostInnerTable = leafMostTable = table;
    }

    public IndexScan(TableSource rootMostTable, 
                     TableSource rootMostInnerTable,
                     TableSource leafMostInnerTable,
                     TableSource leafMostTable) {
        this.rootMostTable = rootMostTable;
        this.rootMostInnerTable = rootMostInnerTable;
        this.leafMostInnerTable = leafMostInnerTable;
        this.leafMostTable = leafMostTable;
    }

    public TableSource getRootMostTable() {
        return rootMostTable;
    }
    public TableSource getRootMostInnerTable() {
        return rootMostInnerTable;
    }
    public TableSource getLeafMostInnerTable() {
        return leafMostInnerTable;
    }
    @Override
    public TableSource getLeafMostTable() {
        return leafMostTable;
    }

    /** Return tables included in the index, leafmost to rootmost. */
    public List<TableSource> getTables() {
        List<TableSource> tables = new ArrayList<>();
        TableSource table = leafMostTable;
        while (true) {
            tables.add(table);
            if (table == rootMostTable) break;
            table = table.getParentTable();
        }
        return tables;
    }

    public boolean isCovering() {
        return covering;
    }
    public void setCovering(boolean covering) {
        this.covering = covering;
    }

    public CostEstimate getScanCostEstimate() {
        return scanCostEstimate;
    }

    public void setScanCostEstimate(CostEstimate scanCostEstimate) {
        this.scanCostEstimate = scanCostEstimate;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof ExpressionRewriteVisitor) {
                visitComparands((ExpressionRewriteVisitor)v);
            }
            else if (v instanceof ExpressionVisitor) {
                visitComparands((ExpressionVisitor)v);
            }
            // Don't visit any tables here; they are done by a lookup when that's needed.
        }
        return v.visitLeave(this);
    }
    
    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
    }

    @Override
    public int getPeggedCount() {
        return getNEquality();
    }

    public abstract List<OrderByExpression> getOrdering();
    public abstract OrderEffectiveness getOrderEffectiveness();
    public abstract List<ExpressionNode> getColumns();
    public abstract List<IndexColumn> getIndexColumns();
    public abstract int getNKeyColumns();
    public abstract boolean usesAllColumns();
    public abstract void setUsesAllColumns(boolean usesAllColumns);
    public abstract List<ExpressionNode> getEqualityComparands();
    public abstract List<ConditionExpression> getConditions();
    public abstract boolean hasConditions();
    public abstract ExpressionNode getLowComparand();
    public abstract boolean isLowInclusive();
    public abstract ExpressionNode getHighComparand();
    public abstract boolean isHighInclusive();
    public abstract boolean isSpatial();

    /**
     * The index of the first column that may have more than one result
     * NOTE: by default Unions are included in this count.
     */
    public abstract int getNEquality();
    /**
     * The number of unions that come after the true equalities.
     */
    public abstract int getNUnions();

    /**
     * By Default a union (i.e. UNION of [% = 30, % = 50, % = 90]) is considered
     * to be an equality, and thus can be turned into UnionOrdered. If there is an OrderBy or MIN on that column,
     * we need to compare the union column and not skip over it. Setting this to false means that the union column
     * is not included in NEquality.
     */
    public abstract void setIncludeUnionAsEquality(boolean sortColumn);
    public abstract boolean isAscendingAt(int index);
    public abstract boolean isRecoverableAt(int index);
    
    public static final boolean INCLUDE_SCAN_COST_IN_SUMMARY = false;

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        return summaryString(-1, configuration);
    }
    
    public String summaryString(boolean prettyFormat, SummaryConfiguration configuration) {
        return summaryString(prettyFormat ? 0 : -1, configuration);
    }

    private String summaryString(int indentation, SummaryConfiguration configuration) {
        StringBuilder sb = new StringBuilder();
        buildSummaryString(sb, indentation, true, configuration);
        return sb.toString();
    }
     
    protected void buildSummaryString(StringBuilder str, int indentation, boolean full,
                                      SummaryConfiguration configuration) {
        str.append(super.summaryString(configuration));
        str.append("(");
        if (configuration.includeIndexTableNames) {
            str.append(" on [");
            for (TableSource table : getTables()) {
                str.append(table.getName());
                str.append(", ");
            }
            str.replace(str.length() - 2, str.length(), "] ");
        }
        str.append(summarizeIndex(indentation, configuration));
        if (indentation < 0) {
            str.append(", ");
        }
        else if (indentation == 0) {
            str.append(Strings.NL);
            indent(str, 1).append("-> ");
        }
        if (full && covering)
            str.append("covering/");
        if (full && getOrderEffectiveness() != null)
            str.append(getOrderEffectiveness());
        if (full && getOrdering() != null) {
            boolean anyReverse = false, allReverse = true;
            for (int i = 0; i < getOrdering().size(); i++) {
                if (getOrdering().get(i).isAscending() != isAscendingAt(i))
                    anyReverse = true;
                else
                    allReverse = false;
            }
            if (anyReverse) {
                if (allReverse)
                    str.append("/reverse");
                else {
                    for (int i = 0; i < getOrdering().size(); i++) {
                        str.append((i == 0) ? "/" : ",");
                        str.append(getOrdering().get(i).isAscending() ? "ASC" : "DESC");
                    }
                }
            }
        }
        describeEqualityComparands(str);
        if (isSpatial()) {
            if (getLowComparand() != null) {
                str.append(", @");
                if (getHighComparand() == null)
                    str.append("@");
                str.append(getLowComparand());
            }
        }
        else {
            if (getLowComparand() != null) {
                str.append(", ");
                str.append((isLowInclusive()) ? ">=" : ">");
                str.append(getLowComparand());
            }
            if (getHighComparand() != null) {
                str.append(", ");
                str.append((isHighInclusive()) ? "<=" : "<");
                str.append(getHighComparand());
            }
        }
        describeConditionRange(str);
        if (full && getCostEstimate() != null) {
            str.append(", ");
            if (INCLUDE_SCAN_COST_IN_SUMMARY && 
                !getCostEstimate().equals(scanCostEstimate)) {
                str.append(scanCostEstimate);
                str.append(" -> ");
            }
            str.append(getCostEstimate());
        }
        str.append(")");
    }
    
    protected static StringBuilder indent(int indentation) {
        return indent(new StringBuilder(), indentation);
    }
    
    protected static StringBuilder indent(StringBuilder str, int indentation) {
        while (indentation --> 0) {
            str.append("    ");
        }
        return str;
    }

    protected abstract String summarizeIndex(int indentation, SummaryConfiguration configuration);
    protected void describeConditionRange(StringBuilder output) {}
    protected void describeEqualityComparands(StringBuilder output) {}
}
