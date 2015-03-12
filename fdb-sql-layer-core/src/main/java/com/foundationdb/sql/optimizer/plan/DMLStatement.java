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

import java.util.List;

import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.optimizer.rule.EquivalenceFinder;

public class DMLStatement extends BaseStatement {

    public DMLStatement(PlanNode input, BaseUpdateStatement.StatementType type, 
                        TableSource selectTable,
                        TableNode targetTable,
                        List<ResultField> results,
                        TableSource returningTable,
                        EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(input, columnEquivalencies);
        this.type = type;
        this.selectTable = selectTable;
        this.targetTable = targetTable;
        this.results = results;
        this.returningTable = returningTable;
    }
    
    public BaseUpdateStatement.StatementType getType() {
        return type;
    }
    
    public List<ResultField> getResultField() {
        return results;
    }

    public TableSource getSelectTable() { 
        return selectTable;
    }
    public TableSource getReturningTable() { 
        return returningTable;
    }

    public TableNode getTargetTable() {
        return targetTable;
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append('(');
        str.append(targetTable);
        str.append(')');
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
    }
    
    private final BaseUpdateStatement.StatementType type; 
    private final List<ResultField> results;
    private final TableSource selectTable, returningTable;
    private TableNode targetTable;

}
