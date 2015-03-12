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
package com.foundationdb.sql.pg;

import com.foundationdb.sql.server.ServerCallExplainer;
import com.foundationdb.sql.server.ServerCallInvocation;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.SparseArrayQueryBindings;
import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.loadableplan.LoadableOperator;
import com.foundationdb.qp.loadableplan.LoadablePlan;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Explainable;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PostgresLoadablePlan
{
    public static PostgresStatement statement(PostgresServerSession server, 
                                              ServerCallInvocation invocation) {
        LoadablePlan<?> loadablePlan = 
            server.getRoutineLoader().loadLoadablePlan(server.getSession(),
                                                       invocation.getRoutineName());
        List<String> columnNames = loadablePlan.columnNames();
        List<PostgresType> columnTypes = columnTypes(loadablePlan,
                                                     server.typesTranslator());
        List<Column> aisColumns = Collections.nCopies(columnNames.size(), null);
        if (loadablePlan instanceof LoadableOperator)
            return new PostgresLoadableOperator((LoadableOperator)loadablePlan, 
                                                invocation,
                                                columnNames, columnTypes, aisColumns,
                                                null);
        if (loadablePlan instanceof LoadableDirectObjectPlan)
            return new PostgresLoadableDirectObjectPlan((LoadableDirectObjectPlan)loadablePlan, 
                                                        invocation,
                                                        columnNames, columnTypes, aisColumns,
                                                        null);
        return null;
    }

    public static QueryBindings setParameters(QueryBindings bindings, ServerCallInvocation invocation) {
        if (!invocation.parametersInOrder()) {
            if (invocation.hasParameters()) {
                QueryBindings calleeBindings = new SparseArrayQueryBindings();
                invocation.copyParameters(bindings, calleeBindings);
                bindings = calleeBindings;
            }
            else {
                invocation.copyParameters(null, bindings);
            }
        }
        return bindings;
    }

    public static List<PostgresType> columnTypes(LoadablePlan<?> plan,
                                                 TypesTranslator typesTranslator)
    {
        List<PostgresType> columnTypes = new ArrayList<>();
        for (int jdbcType : plan.jdbcTypes()) {
            DataTypeDescriptor sqlType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType);
            TInstance type = typesTranslator.typeForSQLType(sqlType);
            columnTypes.add(PostgresType.fromDerby(sqlType, type));
        }
        return columnTypes;
    }

    public static Explainable explainable(PostgresServerSession server, 
                                          final ServerCallInvocation invocation) {
        final LoadablePlan<?> loadablePlan = 
            server.getRoutineLoader().loadLoadablePlan(server.getSession(),
                                                       invocation.getRoutineName());
        return new Explainable() {
                @Override
                public CompoundExplainer getExplainer(ExplainContext context) {
                    Attributes atts = new Attributes();
                    atts.put(Label.PROCEDURE_IMPLEMENTATION, 
                             PrimitiveExplainer.getInstance(loadablePlan.getClass().getName()));
                    return new ServerCallExplainer(invocation, atts, context);
                }
            };
    }

    // All static methods.
    private PostgresLoadablePlan() {
    }
}
