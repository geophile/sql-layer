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
package com.foundationdb.server.types.aksql.aktypes;

import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TClassBase;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;

import java.sql.Types;
import java.util.List;

public class AkResultSet extends TClassBase {
    public static class Column {
        private final String name;
        private final TInstance type;
        
        public Column(String name, TInstance type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public TInstance getType() {
            return type;
        }
    }

    private static final TClassFormatter NO_FORMATTER = new TClassFormatter() {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                throw new UnsupportedOperationException();
            }
        };

    private static final TParser NO_PARSER = new TParser() {
            @Override
            public void parse(TExecutionContext context, ValueSource in, ValueTarget out) {
                throw new UnsupportedOperationException();
            }
        };

    /**
     * A result set instance, which does not obey all of the scalar type protocol.
     */
    public static final AkResultSet INSTANCE = new AkResultSet();

    private AkResultSet() {
        super(AkBundle.INSTANCE.id(),
              "result set",
              AkCategory.RECORD,
              Attribute.NONE.class,
              NO_FORMATTER,
              1,
              1,
              0,
              null, // UnderlyingType.XXX
              NO_PARSER,
              -1);
    }

    public TInstance instance(List<Column> columns) {
        TInstance instance = createInstanceNoArgs(false);
        instance.setMetaData(columns);
        return instance;
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int jdbcType() {
        return Types.OTHER;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected DataTypeDescriptor dataTypeDescriptor(TInstance type) {
        List<Column> columns = (List<Column>) type.getMetaData();
        String[] columnNames = new String[columns.size()];
        DataTypeDescriptor[] columnTypes = new DataTypeDescriptor[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnNames[i] = columns.get(i).getName();
            columnTypes[i] = columns.get(i).getType().dataTypeDescriptor();
        }
        TypeId typeId = new TypeId.RowMultiSetTypeId(columnNames, columnTypes);
        Boolean isNullable = type.nullability();
        return new DataTypeDescriptor(typeId, isNullable);
    }

    @Override
    public TInstance instance(boolean nullable) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void validate(TInstance type) {
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        throw new UnsupportedOperationException();
    }

    public TClass widestComparable()
    {
        return this;
    }
}
