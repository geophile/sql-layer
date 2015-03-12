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
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.List;
import java.util.Arrays;
import java.util.Set;

/** Physical SELECT query */
public class PhysicalSelect extends BasePlannable
{
    // Probably subclassed by specific client to capture typing information in some way.
    public static class PhysicalResultColumn {
        private String name;
        
        public PhysicalResultColumn(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public PhysicalSelect(Operator resultOperator, RowType rowType,
                          List<PhysicalResultColumn> resultColumns,
                          ParameterType[] parameterTypes,
                          CostEstimate costEstimate,
                          Set<Table> affectedTables) {
        super(resultOperator, parameterTypes, rowType, resultColumns, costEstimate, affectedTables);
    }

    public Operator getResultOperator() {
        return (Operator)getPlannable();
    }


    @Override
    public boolean isUpdate() {
        return false;
    }
    
    @Override
    protected String withIndentedExplain(StringBuilder str, ExplainContext context, String defaultSchemaName, DefaultFormatter.LevelOfDetail levelOfDetail) {
        if (getParameterTypes() != null)
            str.append(Arrays.toString(getParameterTypes()));
        str.append(getResultColumns());
        return super.withIndentedExplain(str, context, defaultSchemaName, levelOfDetail);
    }

}
