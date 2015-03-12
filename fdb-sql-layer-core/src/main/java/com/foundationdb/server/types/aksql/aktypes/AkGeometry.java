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

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.UnsupportedSpatialCast;
import com.foundationdb.server.spatial.Spatial;
import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TClassBase;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Types;

/** An internal, runtime only wrapper around JTS Geometry objects. */
public class AkGeometry extends TClassBase
{
    public static final TClass INSTANCE = new AkGeometry();

    private static final String NAME = "GEOMETRY";
    private static final TypeId TYPE_ID;
    private static final DataTypeDescriptor DATA_TYPE_DESCRIPTOR;

    static {
        try {
            TYPE_ID =  TypeId.getUserDefinedTypeId(NAME, false);
            DATA_TYPE_DESCRIPTOR = new DataTypeDescriptor(TYPE_ID, true);
        } catch(StandardException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class GeometryParser implements TParser
    {
        @Override
        public void parse(TExecutionContext context, ValueSource in, ValueTarget out) {
            throw new UnsupportedSpatialCast();
        }
    }

    private static class GeometryFormatter implements TClassFormatter
    {
        @Override
        public void format(TInstance type, ValueSource source, AkibanAppender out) {
            out.append(source.getObject().toString());
        }

        @Override
        public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
            StringWriter sw = new StringWriter();
            try {
                new WKTWriter(Spatial.LAT_LON_DIMENSIONS).write((Geometry)source.getObject(),
                                                                sw);
            }
            catch (IOException ex) {
                throw new AkibanInternalException("Error formatting to string", ex);
            }
            out.append('\'');
            out.append(sw.toString());
            out.append('\'');
        }

        @Override
        public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
            // TODO: GeoJSON?
            throw new UnsupportedSpatialCast();
        }
    }

    private AkGeometry() {
        super(AkBundle.INSTANCE.id(),
              NAME,
              null /*category*/,
              Attribute.NONE.class,
              new GeometryFormatter(),
              1 /*internal version*/,
              1 /*version*/,
              0 /*size*/,
              null /*underlying type*/,
              new GeometryParser(),
              -1 /*default varchar len*/);
    }

    @Override
    public int jdbcType() {
        return Types.OTHER;
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance type) {
        return DATA_TYPE_DESCRIPTOR;
    }

    @Override
    public TClass widestComparable() {
        return this;
    }

    @Override
    protected boolean attributeIsPhysical(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TInstance instance(boolean nullable) {
        return createInstanceNoArgs(nullable);
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        return createInstanceNoArgs(suggestedNullability);
    }

    @Override
    protected void validate(TInstance type) {
    }
}
