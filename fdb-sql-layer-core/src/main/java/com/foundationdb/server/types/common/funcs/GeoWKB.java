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
import com.foundationdb.server.error.InvalidArgumentTypeException;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

/** Parse a Well-Known binary (WKB) byte string into a geometry object. Thin wrapper around {@link WKBReader}. */
public class GeoWKB extends TScalarBase
{
    private static final int READER_CONTEXT_POS = 0;

    private final TClass binaryType;
    private final TClass geometryType;

    public GeoWKB(TClass binaryType, TClass geometryType) {
        this.binaryType = binaryType;
        this.geometryType = geometryType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(binaryType, 0);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        context.set(READER_CONTEXT_POS, new WKBReader());
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        byte[] data = new byte[0];

        if (inputs.get(0).hasAnyValue()) {
            Object o = inputs.get(0).getObject();
            if (o instanceof BlobRef) {
                BlobRef blob;
                blob = (BlobRef) o;
                String mode = context.getQueryContext().getStore().getConfig().getProperty(AkBlob.RETURN_UNWRAPPED);
                if (mode.equalsIgnoreCase(AkBlob.UNWRAPPED)){
                    data = blob.getBytes();
                }
                else {
                    if (blob.isShortLob()) {
                        data = blob.getBytes();
                    } else {
                        LobService ls = context.getQueryContext().getServiceManager().getServiceByClass(LobService.class);
                        data = ls.readBlob(context.getQueryContext().getSession(), blob.getId());
                    }
                }
            } else if (o instanceof byte[]) {
                data = (byte[])o;
            }
        }
        
        WKBReader reader = (WKBReader)context.preptimeObjectAt(READER_CONTEXT_POS);
        try {
            Geometry geometry = reader.read(data);
            output.putObject(geometry);
        } catch(ParseException e) {
            throw new InvalidSpatialObjectException(e.getMessage());
        }
    }

    @Override
    public String displayName() {
        return "GEO_WKB";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(geometryType);
    }
}
