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
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/** Physical INSERT/UPDATE/DELETE statement */
public class PhysicalUpdate extends BasePlannable
{
    private boolean returning;
    private boolean putInCache;

    public PhysicalUpdate(Operator resultsOperator, 
                          ParameterType[] parameterTypes,
                          RowType rowType, 
                          List<PhysicalResultColumn> resultColumns,
                          boolean returning, 
                          boolean putInCache,
                          CostEstimate costEstimate,
                          Set<Table> affectedTables) {
        super (resultsOperator, parameterTypes, rowType, resultColumns, costEstimate, affectedTables);
        this.returning = returning;
        this.putInCache = putInCache;
    }
    
    public boolean isReturning() {
        return returning;
    }

    public boolean putInCache() {
        return putInCache;
    }

    @Override
    public boolean isUpdate() {
        return true;
    }

    @Override
    protected String withIndentedExplain(StringBuilder str, ExplainContext context, String defaultSchemaName, DefaultFormatter.LevelOfDetail levelOfDetail) {
        if (getParameterTypes() != null)
            str.append(Arrays.toString(getParameterTypes()));
        if (!putInCache)
            str.append("/NO_CACHE");
        return super.withIndentedExplain(str, context, defaultSchemaName, levelOfDetail);
    }

}
