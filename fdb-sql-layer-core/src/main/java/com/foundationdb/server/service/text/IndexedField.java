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
package com.foundationdb.server.service.text;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

import java.sql.Types;

public class IndexedField
{
    public static final String KEY_FIELD = ".hkey";

    public static enum FieldType {
        INT, LONG, FLOAT, DOUBLE, STRING, TEXT
    }

    private final Column column;
    private final int position;
    private final String name;
    private final FieldType fieldType;
    
    public IndexedField(Column column) {
        this.column = column;

        position = column.getPosition();
        name = column.getName(); // TODO: Need to make unique among multiple tables.
        
        switch (column.getType().typeClass().jdbcType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
            fieldType = FieldType.INT;
            break;
        case Types.BIGINT:
            fieldType = FieldType.LONG;
            break;
        case Types.FLOAT:
        case Types.REAL:
            fieldType = FieldType.FLOAT;
            break;
        case Types.DOUBLE:
            fieldType = FieldType.DOUBLE;
            break;
        case Types.CHAR:
        case Types.NVARCHAR:
        case Types.VARCHAR:
            {
                AkCollator collator = column.getCollator();
                if ((collator == null) || collator.isCaseSensitive()) {
                    fieldType = FieldType.STRING;
                }
                else {
                    fieldType = FieldType.TEXT;
                }
            }
            break;
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.CLOB:
            fieldType = FieldType.TEXT;
            break;
        default:
            fieldType = FieldType.STRING;
            break;
        }
    }

    public Column getColumn() {
        return column;
    }

    public int getPosition() {
        return position;
    }

    public String getName() {
        return name;
    }

    public boolean isCasePreserving() {
        return (fieldType != FieldType.TEXT);
    }

    public Field getField(ValueSource value) {
        if (value.isNull()) 
            return null;
        Field.Store store = Field.Store.NO; // Only store hkey.
        switch (fieldType) {
        case INT:
            switch (TInstance.underlyingType(value.getType())) {
            case INT_8:
                return new IntField(name, value.getInt8(), store);
            case INT_16:
                return new IntField(name, value.getInt16(), store);
            case UINT_16:
                return new IntField(name, value.getUInt16(), store);
            case INT_32:
            default:
                return new IntField(name, value.getInt32(), store);
            }
        case LONG:
            return new LongField(name, value.getInt64(), store);
        case FLOAT:
            return new FloatField(name, value.getFloat(), store);
        case DOUBLE:
            return new DoubleField(name, value.getDouble(), store);
        case STRING:
            switch (TInstance.underlyingType(value.getType())) {
            case STRING:
                return new StringField(name, value.getString(), store);
            default:
                {
                    StringBuilder str = new StringBuilder();
                    value.getType().format(value, AkibanAppender.of(str));
                    return new StringField(name, str.toString(), store);
                }
            }
        case TEXT:
            return new TextField(name, value.getString(), store);
        default:
            return null;
        }
    }

    @Override
    public String toString() {
        return name;
    }

}
