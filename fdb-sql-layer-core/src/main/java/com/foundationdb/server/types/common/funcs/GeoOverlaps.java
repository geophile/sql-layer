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

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.vividsolutions.jts.geom.Geometry;

/** Simple predicates between two {@link Geometry} objects. */
public class GeoOverlaps extends TScalarBase
{
    public enum OverlapType {
        GEO_OVERLAPS, GEO_CONTAINS, GEO_COVERS;

        public String functionName() {
            return name();
        }
    }

    public static TScalar[] create(TClass geometryType) {
        TScalar[] funs = new TScalar[OverlapType.values().length];
        for (int i = 0; i < funs.length; i++) {
            funs[i] = new GeoOverlaps(geometryType, OverlapType.values()[i]);
        }
        return funs;
    }

    private final TClass geometryType;
    private final OverlapType overlapType;

    public GeoOverlaps(TClass geometryType, OverlapType overlapType) {
        this.geometryType = geometryType;
        this.overlapType = overlapType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(geometryType, 0).covers(geometryType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        Geometry geo1 = (Geometry)inputs.get(0).getObject();
        Geometry geo2 = (Geometry)inputs.get(1).getObject();
        boolean result = false;
        switch (overlapType) {
        case GEO_OVERLAPS:
            result = geo1.overlaps(geo2);
            break;
        case GEO_CONTAINS:
            result = geo1.contains(geo2);
            break;
        case GEO_COVERS:
            result = geo1.covers(geo2);
            break;
        }
        output.putBool(result);
    }

    @Override
    public String displayName() {
        return overlapType.functionName();
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBool.INSTANCE);
    }
}
