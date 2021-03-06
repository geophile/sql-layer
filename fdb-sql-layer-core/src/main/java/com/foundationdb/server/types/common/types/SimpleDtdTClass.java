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

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

public abstract class SimpleDtdTClass extends TClassBase {

    protected <A extends Enum<A> & Attribute>SimpleDtdTClass(TBundleID bundle, String name, Enum<?> category, TClassFormatter formatter, Class<A> enumClass, int internalRepVersion,
                              int serializationVersion, int serializationSize, UnderlyingType underlyingType, TParser parser, int defaultVarcharLen, TypeId typeId) {
        super(bundle, name, category, enumClass, formatter, internalRepVersion, serializationVersion, serializationSize, underlyingType, parser, defaultVarcharLen);
        this.typeId = typeId;
    }

    @Override
    public int jdbcType() {
        return typeId.getJDBCTypeId();
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance type) {
        boolean isNullable = type.nullability(); // on separate line to make NPE easier to catch
        return new DataTypeDescriptor(typeId, isNullable);
    }

    private final TypeId typeId;
}
