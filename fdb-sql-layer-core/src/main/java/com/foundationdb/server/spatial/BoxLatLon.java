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
package com.foundationdb.server.spatial;

import com.foundationdb.server.error.OutOfRangeException;
import com.geophile.z.SpatialObject;
import com.geophile.z.spatialobject.d2.Box;

public abstract class BoxLatLon
{
    public static SpatialObject newBox(double latLo,
                                       double latHi,
                                       double lonLo,
                                       double lonHi)
    {
        latLo = fixLat(latLo);
        latHi = fixLat(latHi);
        lonLo = fixLon(lonLo);
        lonHi = fixLon(lonHi);
        try {
            return
                lonLo <= lonHi
                ? new Box(latLo, latHi, lonLo, lonHi)
                : new BoxLatLonWithWraparound(latLo, latHi, lonLo, lonHi);
        } catch (IllegalArgumentException e) {
            throw new OutOfRangeException(String.format("latLo = %s, latHi = %s, lonLo = %s, lonHi = %s",
                                                        latLo, latHi, lonLo, lonHi));
        }
    }

    // Query boxes are specified as center point += delta, delta <= 360. This calculation can put us past min/max lon.
    private static double fixLon(double lon)
    {
        if (lon > Spatial.MAX_LON + CIRCLE || lon < Spatial.MIN_LON - CIRCLE) {
            throw new OutOfRangeException(String.format("longitude %s", lon));
        }
        if (lon < Spatial.MIN_LON) {
            lon += CIRCLE;
        } else if (lon > Spatial.MAX_LON) {
            lon -= CIRCLE;
        }
        return lon;
    }

    // Fix lat by truncating at +/-90
    private static double fixLat(double lat)
    {
        if (lat > Spatial.MAX_LAT + CIRCLE || lat < Spatial.MIN_LAT - CIRCLE) {
            throw new OutOfRangeException(String.format("latitude %s", lat));
        }
        if (lat > Spatial.MAX_LAT) {
            lat = Spatial.MAX_LAT;
        } else if (lat < Spatial.MIN_LAT) {
            lat = Spatial.MIN_LAT;
        }
        return lat;
    }

    private static final double CIRCLE = 360;
}
