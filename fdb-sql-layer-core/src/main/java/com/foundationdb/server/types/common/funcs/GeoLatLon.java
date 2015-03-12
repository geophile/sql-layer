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
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import static com.foundationdb.server.types.common.funcs.DistanceLatLon.*;

/** Construct a Point {@link Geometry} object from two coordinates. */
public class GeoLatLon extends TScalarBase
{
    private static final int FACTORY_CONTEXT_POS = 0;

    private final TClass coordType;
    private final TClass geometryType;
    
    public GeoLatLon(TClass coordType, TClass geometryType) {
        this.coordType = coordType;
        this.geometryType = geometryType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(coordType, 0).covers(coordType, 1);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        context.set(FACTORY_CONTEXT_POS, new GeometryFactory());
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        ValueSource input0 = inputs.get(0);
        ValueSource input1 = inputs.get(1);

        GeometryFactory factory = (GeometryFactory)context.preptimeObjectAt(FACTORY_CONTEXT_POS);
        double lat = doubleInRange(TBigDecimal.getWrapper(input0, input0.getType()), MIN_LAT, MAX_LAT);
        double lon = doubleInRange(TBigDecimal.getWrapper(input1, input1.getType()), MIN_LON, MAX_LON);
        Geometry geometry = factory.createPoint(new Coordinate(lat, lon));
        output.putObject(geometry);
    }

    @Override
    public String displayName() {
        return "GEO_LAT_LON";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(geometryType);
    }
}
