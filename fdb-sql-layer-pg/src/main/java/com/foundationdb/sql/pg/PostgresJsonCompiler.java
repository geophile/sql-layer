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

import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.sql.optimizer.NestedResultSetTypeComputer;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect;
import com.foundationdb.sql.optimizer.plan.PhysicalUpdate;
import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.TInstance;

import java.util.*;

public class PostgresJsonCompiler extends PostgresOperatorCompiler
{
    protected PostgresJsonCompiler() {
    }

    @Override
    protected void initAIS(AkibanInformationSchema ais, String defaultSchemaName) {
        super.initAIS(ais, defaultSchemaName);
        binder.setAllowSubqueryMultipleColumns(true);
    }

    @Override
    protected void initTypesRegistry(TypesRegistryService typesRegistry) {
        super.initTypesRegistry(typesRegistry);
        typeComputer = new NestedResultSetTypeComputer(typesRegistry);
    }

    public static PostgresJsonCompiler create(PostgresServerSession server, KeyCreator keyCreator) {
        PostgresJsonCompiler compiler = new PostgresJsonCompiler();
        compiler.initServer(server, keyCreator);
        compiler.initDone();
        return compiler;
    }

    public static class JsonResultColumn extends PhysicalResultColumn {
        private DataTypeDescriptor sqlType;
        private TInstance type;
        private PostgresType pgType;
        private List<JsonResultColumn> nestedResultColumns;
        
        public JsonResultColumn(String name, DataTypeDescriptor sqlType, 
                                TInstance type, PostgresType pgType,
                                List<JsonResultColumn> nestedResultColumns) {
            super(name);
            this.sqlType = sqlType;
            this.type = type;
            this.pgType = pgType;
            this.nestedResultColumns = nestedResultColumns;
        }

        public DataTypeDescriptor getSqlType() {
            return sqlType;
        }

        public TInstance getType() {
            return type;
        }

        public PostgresType getPostgresType() {
            return pgType;
        }

        public List<JsonResultColumn> getNestedResultColumns() {
            return nestedResultColumns;
        }
    }

    @Override
    public PhysicalResultColumn getResultColumn(ResultField field) {
        return getJsonResultColumn(field.getName(), field.getSQLtype(), field.getType());
    }

    protected JsonResultColumn getJsonResultColumn(String name, 
                                                   DataTypeDescriptor sqlType, TInstance type) {
        PostgresType pgType = null;
        List<JsonResultColumn> nestedResultColumns = null;
        if (sqlType == null) {
        }
        else if (sqlType.getTypeId().isRowMultiSet()) {
            TypeId.RowMultiSetTypeId typeId = 
                (TypeId.RowMultiSetTypeId)sqlType.getTypeId();
            String[] columnNames = typeId.getColumnNames();
            DataTypeDescriptor[] columnTypes = typeId.getColumnTypes();
            nestedResultColumns = new ArrayList<>(columnNames.length);
            for (int i = 0; i < columnNames.length; i++) {
                nestedResultColumns.add(getJsonResultColumn(columnNames[i], columnTypes[i],
                        getTypesTranslator().typeForSQLType(columnTypes[i])));
            }
        }
        else {
            pgType = PostgresType.fromDerby(sqlType, type);
        }
        return new JsonResultColumn(name, sqlType, type, pgType, nestedResultColumns);
    }

    @Override
    protected PostgresBaseOperatorStatement generateSelect() {
        return new PostgresJsonStatement(this);
    }

    @Override
    protected PostgresBaseOperatorStatement generateSelect(PostgresStatement pstmt,
                                                           PhysicalSelect select,
                                                           PostgresType[] parameterTypes) {
        PostgresJsonStatement pjstmt = (PostgresJsonStatement)pstmt;
        int ncols = select.getResultColumns().size();
        List<JsonResultColumn> resultColumns = new ArrayList<>(ncols);
        for (PhysicalResultColumn physColumn : select.getResultColumns()) {
            JsonResultColumn resultColumn = (JsonResultColumn)physColumn;
            resultColumns.add(resultColumn);
        }
        pjstmt.init(select.getResultOperator(),
                    select.getResultRowType(),
                    resultColumns,
                    parameterTypes,
                    select.getCostEstimate());
        return pjstmt;
    }

    @Override
    protected PostgresBaseOperatorStatement generateUpdate() {
        return super.generateUpdate(); // To handle !returning, see below
    }

    @Override
    protected PostgresBaseOperatorStatement generateUpdate(PostgresStatement pstmt,
                                                           PhysicalUpdate update, String statementType,
                                                           PostgresType[] parameterTypes) {
        if (!update.isReturning()) {
            return super.generateUpdate(pstmt, update, statementType, parameterTypes);
        }
        else {
            int ncols = update.getResultColumns().size();
            List<JsonResultColumn> resultColumns = new ArrayList<>(ncols);
            for (PhysicalResultColumn physColumn : update.getResultColumns()) {
                JsonResultColumn resultColumn = (JsonResultColumn)physColumn;
                resultColumns.add(resultColumn);
            }
            PostgresJsonModifyStatement pjmstmt = new PostgresJsonModifyStatement(this);
            pjmstmt.init(statementType,
                         (Operator)update.getPlannable(),
                         update.getResultRowType(),
                         resultColumns,
                         parameterTypes,
                         update.getCostEstimate(),
                         update.putInCache());
            return pjmstmt;
        }
    }
}
