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
package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.error.InvalidSpatialObjectException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/** Parse a Well-Known Text (WKT) string into a geometry object. Thin wrapper around {@link WKTReader}. */
public class GeoWKT extends TScalarBase
{
    private static final int READER_CONTEXT_POS = 0;

    private final TClass stringType;
    private final TClass geometryType;

    public GeoWKT(TClass stringType, TClass geometryType) {
        this.stringType = stringType;
        this.geometryType = geometryType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(stringType, 0);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        context.set(READER_CONTEXT_POS, new WKTReader());
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        String wkt = inputs.get(0).getString();
        WKTReader reader = (WKTReader)context.preptimeObjectAt(READER_CONTEXT_POS);
        try {
            Geometry geometry = reader.read(wkt);
            output.putObject(geometry);
        } catch(ParseException e) {
            throw new InvalidSpatialObjectException(e.getMessage());
        }
    }

    @Override
    public String displayName() {
        return "GEO_WKT";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(geometryType);
    }
}
