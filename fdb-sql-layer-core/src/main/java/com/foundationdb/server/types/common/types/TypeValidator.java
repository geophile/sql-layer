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
package com.foundationdb.server.types.common.types;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.*;

import java.sql.Types;

public class TypeValidator
{
    private TypeValidator() {
    }

    // TODO: Some of these are properly constraints on the
    // store and should be revisited.

    public static boolean isSupportedForColumn(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        return ((tclass != null) && isSupportedForColumn(tclass));
    }

    public static boolean isSupportedForColumn(TClass type) {
        return ((type.jdbcType() != Types.OTHER) || (type instanceof AkGUID));
    }

    public static boolean isSupportedForIndex(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        return ((tclass != null) && isSupportedForIndex(tclass));
    }

    public static boolean isSupportedForNonPointSpatialIndex(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        return tclass instanceof TString || tclass instanceof TBinary || tclass instanceof AkBlob;
    }

    public static boolean isSupportedForIndex(TClass type) {
        switch (type.jdbcType()) {
        case Types.BLOB:
        case Types.CLOB:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
            return false;
        default:
            return true;
        }
    }

    public static boolean isSupportedForJoin(TInstance type1, TInstance type2) {
        TClass tclass1 = TInstance.tClass(type1);
        TClass tclass2 = TInstance.tClass(type2);
        return ((tclass1 != null) && (tclass2 != null) &&
                isSupportedForJoin(tclass1, tclass2));
    }
    
    public static boolean isSupportedForJoin(TClass tclass1, TClass tclass2) {
        if (tclass1 == tclass2) {
            return true;
        }
        int jt1 = baseJoinType(tclass1);
        int jt2 = baseJoinType(tclass2);
        if (jt1 == jt2) {
            switch (jt1) {
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.STRUCT:
                return false;
            default:
                return true;
            }
        }
        else {
            return false;
        }
    }

    public static boolean isSupportedForGenerator(TInstance instance) {
        
        switch (instance.typeClass().jdbcType()) {
        case Types.BIGINT:
        case Types.INTEGER:
        case Types.SMALLINT:
            return true;
        default: 
               return false;
        }
    }
    
    // Want to allow: CHAR & VARCHAR, INT & BIGINT, INT & INT UNSIGNED, etc.
    // TODO: Also allows DATETIME & TIMESTAMP, and even cross-bundle;
    // will that be okay?
    protected static int baseJoinType(TClass tclass) {
        int jdbcType = tclass.jdbcType();
        switch (jdbcType) {
        case Types.BIGINT:
            if (tclass.isUnsigned())
                return Types.OTHER;
        /* else falls through */
        case Types.TINYINT:
        case Types.INTEGER:
        case Types.SMALLINT:
            return Types.BIGINT;
        case Types.NUMERIC:
        case Types.DECIMAL:
            return Types.DECIMAL;
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
            return Types.DOUBLE;
        case Types.CHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.VARCHAR:
            return Types.VARCHAR;
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
            return Types.LONGVARCHAR;
        case Types.BINARY:
        case Types.BIT:
        case Types.VARBINARY:
            return Types.VARBINARY;
        case Types.LONGVARBINARY:
            return Types.LONGVARBINARY;
        default:
            return jdbcType;
        }
    }
}
