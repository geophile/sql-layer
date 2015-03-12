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

/** A statement that modifies the database.
 */
public class BaseUpdateStatement extends BasePlanWithInput
{
    public enum StatementType {
        DELETE,
        INSERT,
        UPDATE
    }
    
    private TableNode targetTable;
    private TableSource table;
    private final StatementType type;
    
    protected BaseUpdateStatement(PlanNode query, StatementType type, TableNode targetTable,
                                    TableSource table) {
        super(query);
        this.type = type;
        this.targetTable = targetTable;
        this.table = table;
    }

    public TableNode getTargetTable() {
        return targetTable;
    }


    public TableSource getTable() { 
        return table;
    }

    public StatementType getType() {
        return type;
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append('(');
        fillSummaryString(str);
        //if (requireStepIsolation)
        //    str.append(", HALLOWEEN");
        str.append(')');
        return str.toString();
    }

    protected void fillSummaryString(StringBuilder str) {
        str.append(getTargetTable());
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        targetTable = map.duplicate(targetTable);
    }
}
