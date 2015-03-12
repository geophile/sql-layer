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

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.*;

/** Physical operator plan */
public abstract class BasePlannable extends BasePlanNode
{
    public static class ParameterType {
        private final DataTypeDescriptor sqlType;
        private final TInstance type;

        public ParameterType(DataTypeDescriptor sqlType, TInstance type) {
            this.sqlType = sqlType;
            this.type = type;
        }

        public DataTypeDescriptor getSQLType() {
            return sqlType;
        }

        public TInstance getType() {
            return type;
        }

        @Override
        public String toString() {
            if (type != null)
                return type.toStringConcise(true);
            else
                return Objects.toString(sqlType);
        }
    }

    private Plannable plannable;
    private ParameterType[] parameterTypes;
    private List<PhysicalResultColumn> resultColumns;
    private RowType rowType;
    private CostEstimate costEstimate;
    private Set<Table> affectedTables;

    protected BasePlannable(Plannable plannable,
                            ParameterType[] parameterTypes,
                            RowType rowType,
                            List<PhysicalResultColumn> resultColumns,
                            CostEstimate costEstimate,
                            Set<Table> affectedTables) {
        this.plannable = plannable;
        this.parameterTypes = parameterTypes;
        this.rowType = rowType;
        this.resultColumns = resultColumns;
        this.costEstimate = costEstimate;
        this.affectedTables = affectedTables;
    }

    public Plannable getPlannable() {
        return plannable;
    }
    public ParameterType[] getParameterTypes() {
        return parameterTypes;
    }

    public RowType getResultRowType() {
        return rowType;
    }

    public List<PhysicalResultColumn> getResultColumns() {
        return resultColumns;
    }

    public CostEstimate getCostEstimate() {
        return costEstimate;
    }

    public Set<Table> getAffectedTables() {
        return affectedTables;
    }

    public abstract boolean isUpdate();

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy operators.
    }
    
    public String explainToString(ExplainContext context, String defaultSchemaName, DefaultFormatter.LevelOfDetail levelOfDetail) {
        return withIndentedExplain(new StringBuilder(getClass().getSimpleName()), context, defaultSchemaName, levelOfDetail);
    }

    @Override
    public String planString(SummaryConfiguration configuration) {
        // Similar to above, but with @hash for consistency and verbose
        return withIndentedExplain(new StringBuilder(super.summaryString(configuration)), null, null, DefaultFormatter.LevelOfDetail.VERBOSE_WITHOUT_COST);
    }

    protected String withIndentedExplain(StringBuilder str, ExplainContext context, String defaultSchemaName, DefaultFormatter.LevelOfDetail levelOfDetail) {
        if (context == null)
            context = new ExplainContext(); // Empty
        DefaultFormatter f = new DefaultFormatter(defaultSchemaName, levelOfDetail);
        for (String operator : f.format(plannable.getExplainer(context))) {
            str.append("\n  ");
            str.append(operator);
        }
        return str.toString();
    }

}
