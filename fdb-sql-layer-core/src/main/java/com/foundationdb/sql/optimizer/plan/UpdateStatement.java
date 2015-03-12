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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.ColumnContainer;

/** A SQL UPDATE statement. */
public class UpdateStatement extends BaseUpdateStatement
{
    /** One of the SET clauses of an UPDATE statement.
     */
    public static class UpdateColumn extends AnnotatedExpression implements ColumnContainer {
        private Column column;

        public UpdateColumn(Column column, ExpressionNode value) {
            super(value);
            this.column = column;
        }

        @Override
        public Column getColumn() {
            return column;
        }

        @Override
        public String toString() {
            return column + " = " + getExpression();
        }
    }

    private List<UpdateColumn> updateColumns;

    public UpdateStatement(PlanNode query, TableNode targetTable,
            List<UpdateColumn> updateColumns,
                           TableSource table) {
        super(query, StatementType.UPDATE, targetTable, table);
        this.updateColumns = updateColumns;
    }


    public List<UpdateColumn> getUpdateColumns() {
        return updateColumns;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (UpdateColumn updateColumn : updateColumns) {
                        updateColumn.accept((ExpressionRewriteVisitor)v);
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (UpdateColumn updateColumn : updateColumns) {
                        if (!updateColumn.accept((ExpressionVisitor)v)) {
                            break;
                        }
                    }
                }
            }
        }
        return v.visitLeave(this);
    }
    

    @Override
    protected void fillSummaryString(StringBuilder str) {
        super.fillSummaryString(str);
        str.append(updateColumns);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        updateColumns = duplicateList(updateColumns, map);
    }

}
