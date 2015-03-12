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

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;

import java.util.*;

public class PostgresJsonStatement extends PostgresOperatorStatement
{
    private List<JsonResultColumn> resultColumns;
    private TInstance colTInstance;

    public PostgresJsonStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
        colTInstance = compiler.getTypesTranslator().typeForString();
    }

    public void init(Operator resultOperator, RowType resultRowType,
                     List<JsonResultColumn> resultColumns,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate) {
        super.init(resultOperator, resultRowType,
                   // Looks like just one unlimited VARCHAR to the client.
                   jsonColumnNames(), jsonColumnTypes(colTInstance), jsonAISColumns(),
                   parameterTypes, costEstimate);
        this.resultColumns = resultColumns;
    }

    public static List<String> jsonColumnNames() {
        return Collections.singletonList("JSON");
    }

    public static List<PostgresType> jsonColumnTypes(TInstance colTInstance) {
        return Collections.singletonList(new PostgresType(PostgresType.TypeOid.JSON_TYPE_OID,
                                                          (short)-1, -1,
                                                          colTInstance));
    }

    public static List<Column> jsonAISColumns() {
        return Collections.singletonList(null);
    }

    @Override
    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresJsonOutputter(context, this, 
                                         resultColumns, getColumnTypes().get(0));
    }
    
}
