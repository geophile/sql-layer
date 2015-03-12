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

import com.foundationdb.sql.optimizer.plan.CostEstimate;
import static com.foundationdb.sql.pg.PostgresJsonCompiler.JsonResultColumn;
import static com.foundationdb.sql.pg.PostgresJsonStatement.jsonColumnNames;
import static com.foundationdb.sql.pg.PostgresJsonStatement.jsonColumnTypes;
import static com.foundationdb.sql.pg.PostgresJsonStatement.jsonAISColumns;

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;

import java.util.*;

public class PostgresJsonModifyStatement extends PostgresModifyOperatorStatement
{
    private List<JsonResultColumn> resultColumns;
    private TInstance colTInstance;

    public PostgresJsonModifyStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
        colTInstance = compiler.getTypesTranslator().typeForString();
    }

    public void init(String statementType, Operator resultOperator, RowType resultRowType,
                     List<JsonResultColumn> resultColumns,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate,
                     boolean putInCache) {
        super.init(statementType, resultOperator, resultRowType,
                   // Looks like just one unlimited VARCHAR to the client.
                   jsonColumnNames(), jsonColumnTypes(colTInstance), jsonAISColumns(),
                   parameterTypes, costEstimate,  
                   putInCache);
        this.resultColumns = resultColumns;
    }

    @Override
    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresJsonOutputter(context, this, 
                                         resultColumns, getColumnTypes().get(0));
    }
    
}
