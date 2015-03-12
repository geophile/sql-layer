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

import com.foundationdb.server.error.UnknownDataTypeException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.optimizer.plan.BasePlannable;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect;
import com.foundationdb.sql.optimizer.plan.PhysicalUpdate;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerPlanContext;

import java.util.List;

public abstract class PostgresBaseOperatorStatement extends PostgresDMLStatement
{
    private PostgresOperatorCompiler compiler;

    protected PostgresBaseOperatorStatement(PostgresOperatorCompiler compiler) {
        this.compiler = compiler;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        DMLStatementNode dmlStmt = (DMLStatementNode)stmt;
        PostgresQueryContext queryContext = new PostgresQueryContext(server);
        PlanContext planContext = new ServerPlanContext(compiler, queryContext);
        // TODO: This needs to make types with better default attributes or else
        // decimals and strings get truncated, collation doesn't match, etc.
        if (paramTypes != null && false) {
            for (ParameterNode param : params) {
                int paramno = param.getParameterNumber();
                if (paramno < paramTypes.length) {
                    TInstance type = null;
                    try {
                        type = server.typesTranslator().typeClassForJDBCType(PostgresType.toJDBC(paramTypes[paramno])).instance(true);
                    }
                    catch (UnknownDataTypeException ex) {
                        server.warnClient(ex);
                    }
                    param.setUserData(type);
                }
            }
        }
        BasePlannable result = compiler.compile(dmlStmt, params, planContext);
        PostgresType[] parameterTypes = getParameterTypes(result.getParameterTypes(),
                                                          paramTypes,
                                                          server.typesTranslator());

        final PostgresBaseOperatorStatement pbos;
        if (result.isUpdate())
            pbos = compiler.generateUpdate(this,
                                           (PhysicalUpdate)result, stmt.statementToString(),
                                           parameterTypes);
        else
            pbos = compiler.generateSelect(this,
                                           (PhysicalSelect)result,
                                           parameterTypes);
        pbos.compiler = null;
        return pbos;
    }

    protected PostgresType[] getParameterTypes(BasePlannable.ParameterType[] planTypes,
                                               int[] paramTypes,
                                               TypesTranslator typesTranslator) {
        if (planTypes == null) 
            return null;
        int nparams = planTypes.length;
        PostgresType[] parameterTypes = new PostgresType[nparams];
        for (int i = 0; i < nparams; i++) {
            BasePlannable.ParameterType planType = planTypes[i];
            PostgresType pgType = null;
            if ((planType != null) && (planType.getType() != null)) {
                pgType = PostgresType.fromTInstance(planType.getType());
            }
            if ((paramTypes != null) && (i < paramTypes.length)) {
                // Make a type that has the target that the query wants, with the
                // OID that the client proposed to send so that we
                // decode it properly.
                PostgresType.TypeOid oid = PostgresType.TypeOid.fromOid(paramTypes[i]);
                if (oid != null) {
                    if (pgType == null)
                        pgType = new PostgresType(oid, (short)-1, -1, null);
                    else
                        pgType = new PostgresType(oid,  (short)-1, -1,
                                                  pgType.getType());
                }
            }
            parameterTypes[i] = pgType;
        }
        return parameterTypes;
    }
}
