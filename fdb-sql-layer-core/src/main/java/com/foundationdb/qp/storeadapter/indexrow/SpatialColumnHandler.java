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
package com.foundationdb.qp.storeadapter.indexrow;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.WriteIndexRow;
import com.foundationdb.server.error.InvalidSpatialObjectException;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.spatial.Spatial;
import com.geophile.z.Space;
import com.geophile.z.SpatialObject;
import com.geophile.z.spatialobject.d2.Point;
import com.vividsolutions.jts.io.ParseException;

public class SpatialColumnHandler
{
    public SpatialColumnHandler(Index index)
    {
        space = index.space();
        dimensions = space.dimensions();
        assert dimensions == 2;
        assert dimensions == index.dimensions();
        firstSpatialField = index.firstSpatialArgument();
        lastSpatialField = index.lastSpatialArgument();
        int spatialColumns = lastSpatialField - firstSpatialField + 1;
        tinstances = new TInstance[spatialColumns];
        positions = new int[spatialColumns];
        for (int c = 0; c < spatialColumns; c++) {
            IndexColumn indexColumn = index.getKeyColumns().get(firstSpatialField + c);
            Column column = indexColumn.getColumn();
            tinstances[c] = column.getType();
            positions[c] = column.getPosition();
        }
        coords = new double[dimensions];
        indexMethod = index.getIndexMethod();
    }

    public boolean handleSpatialColumn(WriteIndexRow writeIndexRow, int indexField, long zValue)
    {
        assert zValue >= 0;
        if (indexField == firstSpatialField) {
            writeIndexRow.pKey().append(zValue);
        }
        return indexField >= firstSpatialField && indexField <= lastSpatialField;
    }

    public void processSpatialObject(Row row, Operation operation)
    {
        bind(row);
        if (spatialObject != null) {
            long[] zs = zArray();
            Spatial.shuffle(space, spatialObject, zs);
            for (int i = 0; i < zs.length && zs[i] != Space.Z_NULL; i++) {
                operation.handleZValue(zs[i]);
            }
        }
    }

    private void bind (Row row) {
        if (lastSpatialField > firstSpatialField) {
            assert indexMethod == Index.IndexMethod.GEO_LAT_LON : indexMethod;
            // Point coordinates stored in two columns
            assert dimensions == 2 : dimensions;
            double coord = Double.NaN;
            double x = Double.NaN;
            double y = Double.NaN;
            for (int d = 0; d < dimensions; d++) {
                ValueSource source = row.value(positions[d]);
                if (!source.isNull()) {
                    TClass tclass = source.getType().typeClass();
                    if (tclass == MNumeric.DECIMAL) {
                        BigDecimalWrapper wrapper = TBigDecimal.getWrapper(source, tinstances[d]);
                        coord = wrapper.asBigDecimal().doubleValue();
                    }
                    else if (tclass == MNumeric.BIGINT) {
                        coord = source.getInt64();
                    }
                    else if (tclass == MNumeric.INT) {
                        coord = source.getInt32();
                    }
                    else {
                        assert false : row.rowType().table().getColumn(positions[d]);
                    }
                    if (d == 0) {
                        x = coord;
                    } else {
                        y = coord;
                    }
                    coords[d] = coord;
                }
            }
            spatialObject =
                Double.isNaN(x) || Double.isNaN(y)
                ? null
                : new Point(x, y);
        } else {
            ValueSource source = row.value(positions[0]);
            if (source.isNull()) {
                spatialObject = null;
            } else {
                try {
                    switch (indexMethod) {
                        case GEO_WKB:
                            byte[] spatialObjectBytes = ((BlobRef)source.getObject()).getBytes();
                            spatialObject = Spatial.deserializeWKB(space, spatialObjectBytes);
                            break;
                        case GEO_WKT:
                            String spatialObjectText = source.getString();
                            spatialObject = Spatial.deserializeWKT(space, spatialObjectText);
                            break;
                        default:
                            assert false : indexMethod;
                    }
                } catch (ParseException e) {
                    throw new InvalidSpatialObjectException();
                }
            }
        }
    }

    private long[] zArray()
    {
        assert spatialObject != null;
        int maxZ = spatialObject.maxZ();
        if (zs == null || maxZ > zs.length) {
            zs = new long[maxZ];
        }
        return zs;
    }

    private final Space space;
    private final int dimensions;
    private final int[] positions;    
    private final TInstance[] tinstances;
    private final int firstSpatialField;
    private final int lastSpatialField;
    private SpatialObject spatialObject;
    private long[] zs;
    private final double[] coords;
    private final Index.IndexMethod indexMethod;

    // Inner classes

    public static abstract class Operation
    {
        public abstract void handleZValue(long z);
    }
}
