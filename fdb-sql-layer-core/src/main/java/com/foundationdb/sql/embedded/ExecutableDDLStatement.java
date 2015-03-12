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
package com.foundationdb.sql.embedded;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.sql.aisddl.AISDDL;
import com.foundationdb.sql.parser.DDLStatementNode;

class ExecutableDDLStatement extends ExecutableStatement
{
    private DDLStatementNode ddl;
    private String sql;

    protected ExecutableDDLStatement(DDLStatementNode ddl, String sql) {
        this.ddl = ddl;
        this.sql = sql;
    }

    @Override
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
        AISDDL.execute(ddl, sql, context);
        return new ExecuteResults();
    }

    @Override
    public StatementTypes getStatementType() {
        return StatementTypes.DDL_STMT;
    }
    
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.IMPLICIT_COMMIT;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

}
