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

import com.foundationdb.ais.model.ColumnContainer;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.ais.model.Column;

import java.util.List;

/** Name the columns in a SELECT. */
public class ResultSet extends BasePlanWithInput
{
    public static class ResultField extends BaseDuplicatable implements ColumnContainer {

        private String name;
        private DataTypeDescriptor sqlType;
        private Column aisColumn;
        private TInstance type;

        public ResultField(String name, DataTypeDescriptor sqlType, Column aisColumn) {
            this.name = name;
            this.sqlType = sqlType;
            this.aisColumn = aisColumn;
        }

        public ResultField(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public DataTypeDescriptor getSQLtype() {
            if (sqlType == null && type != null) {
                sqlType = type.dataTypeDescriptor();
            }
            return sqlType;
        }

        public Column getAIScolumn() {
            return aisColumn;
        }

        public TInstance getType() {
            return type;
        }

        public void setType(TInstance type) {
            this.type = type;
            if (type != null)
                sqlType = null;
        }

        @Override
        public Column getColumn() {
            return aisColumn;
        }

        public String toString() {
            return name;
        }
    }

    private List<ResultField> fields;

    public ResultSet(PlanNode input, List<ResultField> fields) {
        super(input);
        this.fields = fields;
    }

    public List<ResultField> getFields() {
        return fields;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            getInput().accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder stringBuilder = new StringBuilder(super.summaryString(configuration));
        if (configuration.includeRowTypes) {
            stringBuilder.append('[');
            for (ResultField field : fields) {
                stringBuilder.append(field);
                stringBuilder.append(" (");
                stringBuilder.append(field.getType());
                stringBuilder.append("), ");
            }
            if (fields.size() > 0) {
                stringBuilder.setLength(stringBuilder.length()-2);
            }
            stringBuilder.append(']');
        } else {
            stringBuilder.append(fields);
        }
        return stringBuilder.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        fields = duplicateList(fields, map);
    }

}
