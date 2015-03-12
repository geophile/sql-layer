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

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import java.util.List;

public class JDBCParameterMetaData implements ParameterMetaData
{
    protected static class ParameterType {
        private String name;    // null for ordinary (non-CALL) prepared statements
        private DataTypeDescriptor sqlType;
        private int jdbcType;
        private TInstance type;
        private int mode;       // parameterModeXxx (In for non-CALL)

        protected ParameterType(Parameter param, DataTypeDescriptor sqlType,
                                int jdbcType, TInstance type) {
            this.sqlType = sqlType;
            this.jdbcType = jdbcType;
            this.type = type;

            name = param.getName();
            switch (param.getDirection()) {
            case IN:
                mode = parameterModeIn;
                break;
            case OUT:
            case RETURN:
                mode = parameterModeOut;
                break;
            case INOUT:
                mode = parameterModeInOut;
                break;
            }
        }

        protected ParameterType(DataTypeDescriptor sqlType,
                                int jdbcType, TInstance type) {
            this.sqlType = sqlType;
            this.jdbcType = jdbcType;
            this.type = type;

            mode = parameterModeIn;
        }

        public String getName() {
            return name;
        }

        public DataTypeDescriptor getSQLType() {
            return sqlType;
        }

        public int getJDBCType() {
            return jdbcType;
        }

        public TInstance getType() {
            return type;
        }

        public int getScale() {
            return sqlType.getScale();
        }

        public int getPrecision() {
            return sqlType.getPrecision();
        }

        public boolean isNullable() {
            return sqlType.isNullable();
        }

        public String getTypeName() {
            return sqlType.getTypeName();
        }

        public int getMode() {
            return mode;
        }
    }
    
    private final TypesTranslator typesTranslator;
    private final List<ParameterType> params;

    protected JDBCParameterMetaData(TypesTranslator typesTranslator,
                                    List<ParameterType> params) {
        this.typesTranslator = typesTranslator;
        this.params = params;
    }

    protected List<ParameterType> getParameters() {
        return params;
    }

    protected ParameterType getParameter(int param) {
        return params.get(param - 1);
    }

    public String getParameterName(int param) throws SQLException {
        return getParameter(param).getName();
    }

    /* Wrapper */

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /* ParameterMetaData */

    @Override
    public int getParameterCount() throws SQLException {
        return params.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return getParameter(param).isNullable() ? parameterNullable : parameterNoNulls;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return typesTranslator.isTypeSigned(getParameter(param).getType());
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return getParameter(param).getPrecision();
    }

    @Override
    public int getScale(int param) throws SQLException {
        return getParameter(param).getScale();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return getParameter(param).getJDBCType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return getParameter(param).getTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return typesTranslator.jdbcClass(getParameter(param).getType()).getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return getParameter(param).getMode();
    }
}
